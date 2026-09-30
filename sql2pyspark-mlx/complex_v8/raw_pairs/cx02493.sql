WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE term <> '2024T2'
),
enriched AS (
    -- Join students
    SELECT
        s.*,
        st.gpa AS student_gpa,
        st.student_name,
        st.cohort AS student_cohort
    FROM valid s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa < 3.2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(score) OVER (PARTITION BY course_id, student_id ORDER BY student_gpa DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_max_7,
        MIN(score) OVER w_grade_status_or_unknown AS grade_status_or_unknown_min_score,
        ROUND(AVG(student_gpa) OVER (PARTITION BY course_id, student_id ORDER BY student_gpa DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS student_gpa_avg_7,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY grade_status_or_unknown, student_cohort ORDER BY student_gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, student_id ORDER BY student_gpa DESC, id), 3) AS student_gpa_cume_dist,
        MIN(student_gpa) OVER (PARTITION BY course_id, student_id ORDER BY student_gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_student_gpa_so_far,
        SUM(score) OVER (PARTITION BY grade_status_or_unknown, student_cohort ORDER BY student_gpa ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        RANK() OVER (PARTITION BY term ORDER BY student_gpa) AS student_gpa_rank,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROW_NUMBER() OVER w_course_id_student_id_by_stud_2 AS student_gpa_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status_or_unknown, student_cohort ORDER BY student_gpa ASC, id), 3) AS student_gpa_percent_rank
    FROM enriched
    WINDOW w_grade_status_or_unknown AS (PARTITION BY grade_status_or_unknown), w_course_id_student_id_by_stud_2 AS (PARTITION BY course_id, student_id ORDER BY student_gpa DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    grade_status_or_unknown,
    term_grade_status_label,
    student_gpa,
    student_name,
    student_cohort,
    grade_status_or_unknown_min_score,
    first_enrolled_at,
    student_gpa_cume_dist,
    min_student_gpa_so_far,
    score_sum_6_prior,
    student_gpa_rank,
    score_avg_7,
    student_gpa_seq,
    student_gpa_percent_rank
FROM sequenced
WHERE term = '2025T2'
ORDER BY student_gpa_percent_rank DESC
