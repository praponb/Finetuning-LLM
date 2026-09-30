WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE score <= 74.9
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.grade_status_or_unknown,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM cleaned_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.grade_status_or_unknown,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_grade_status,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on
    FROM exploded_detail_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE YEAR(st.enrolled_on) = 2024
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status NOT IN ('PASS', 'WITHDRAWN')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count
    FROM joined s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        grade_status_or_unknown,
        enrollment_id,
        enrollment_term,
        enrollment_grade_status,
        student_enrolled_on,
        enrollment_grade_status_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, enrollment_term ORDER BY student_gpa DESC, id, enrollment_id), 3) AS student_gpa_percent_rank,
        LEAD(student_gpa, 2) OVER (PARTITION BY MONTH(student_enrolled_on) ORDER BY enrollment_grade_status_count, id, enrollment_id) AS next2_student_gpa,
        COUNT(*) OVER w_grade_status_or_unknown AS rows_in_grade_status_or_unknown,
        SUM(student_gpa) OVER (PARTITION BY MONTH(student_enrolled_on) ORDER BY enrollment_grade_status_count, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_student_gpa,
        ROUND(PERCENT_RANK() OVER w_term_enrollment_term_by_stud_2, 3) AS student_gpa_percent_rank_2,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY grade_status_or_unknown), 2) AS student_gpa_vs_grade_status_or_unknown_avg
    FROM with_stats
    WINDOW w_grade_status_or_unknown AS (PARTITION BY grade_status_or_unknown), w_term_enrollment_term_by_stud_2 AS (PARTITION BY term, enrollment_term ORDER BY student_gpa DESC)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        term,
        grade_status_or_unknown,
        enrollment_id,
        enrollment_grade_status,
        student_enrolled_on,
        student_gpa_percent_rank,
        rows_in_grade_status_or_unknown,
        running_student_gpa,
        student_gpa_percent_rank_2,
        student_gpa_vs_grade_status_or_unknown_avg,
        CONCAT_WS('-', enrollment_grade_status, enrollment_term) AS enrollment_grade_status_enrollment_term_label
    FROM windowed
    WHERE student_gpa_percent_rank < 0.7
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, enrollment_id, measure, amount
FROM (
    SELECT id, enrollment_id, student_gpa_percent_rank, student_gpa_percent_rank_2, student_gpa_vs_grade_status_or_unknown_avg
    FROM scored
) src
UNPIVOT (
    amount FOR measure IN (student_gpa_percent_rank, student_gpa_percent_rank_2, student_gpa_vs_grade_status_or_unknown_avg)
) unp
