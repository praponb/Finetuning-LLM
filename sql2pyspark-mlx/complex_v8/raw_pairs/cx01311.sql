WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 57.6) AS score_distance,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
    WHERE course_id IS NOT NULL
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status <> 'INCOMPLETE' THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_grade_status_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at
    FROM scoped s
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_first_enrollment_enrolled_at AS enrollment_grade_status_count_rank,
        ROW_NUMBER() OVER w_first_enrollment_enrolled_at_2 AS enrollment_grade_status_count_seq,
        COUNT(*) OVER w_enrolled_at_month_student_id AS rows_in_enrolled_at_month_student_id,
        SUM(score) OVER (PARTITION BY YEAR(first_enrollment_enrolled_at) ORDER BY enrollment_grade_status_count DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROUND(score / NULLIF(SUM(score) OVER w_enrolled_at_month_student_id, 0), 4) AS score_share_of_enrolled_at_month_student_id
    FROM summarized_rows
    WINDOW w_first_enrollment_enrolled_at AS (PARTITION BY YEAR(first_enrollment_enrolled_at) ORDER BY enrollment_grade_status_count DESC), w_first_enrollment_enrolled_at_2 AS (PARTITION BY YEAR(first_enrollment_enrolled_at) ORDER BY enrollment_grade_status_count DESC, id), w_enrolled_at_month_student_id AS (PARTITION BY MONTH(enrolled_at), student_id)
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.score_distance,
        s.term_prefix,
        s.enrollment_grade_status_count,
        s.enrollment_grade_status_count_rank,
        s.enrollment_grade_status_count_seq,
        s.rows_in_enrolled_at_month_student_id,
        s.score_sum_4,
        s.score_share_of_enrolled_at_month_student_id,
        c.level AS course_level,
        c.dept AS course_dept,
        c.credits AS course_credits,
        st.major AS student_major,
        st.gpa AS student_gpa
    FROM windowed s
    LEFT JOIN courses c ON c.id = s.course_id AND c.level = 'intro'
    LEFT JOIN students st ON st.id = s.student_id
),
summary AS (
    -- Group-level totals
    SELECT
        grade_status,
        enrollment_grade_status_count_seq,
        MAX(enrollment_grade_status_count) AS max_enrollment_grade_status_count,
        COUNT(*) FILTER (WHERE grade_status NOT IN ('INCOMPLETE', 'FAIL')) AS n_grade_status_not_in_incomple,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        ARRAY_SORT(COLLECT_SET(course_dept)) AS course_dept_list,
        COUNT(*) AS row_count
    FROM enriched_course
    GROUP BY grade_status, enrollment_grade_status_count_seq
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        grade_status,
        enrollment_grade_status_count_seq,
        max_enrollment_grade_status_count,
        grade_status_list,
        course_dept_list,
        row_count,
        ROUND(CUME_DIST() OVER (PARTITION BY n_grade_status_not_in_incomple, enrollment_grade_status_count_seq ORDER BY max_enrollment_grade_status_count, grade_status, enrollment_grade_status_count_seq), 3) AS max_enrollment_grade_status_count_cume_dist,
        MAX(row_count) OVER (ORDER BY n_grade_status_not_in_incomple DESC, grade_status, enrollment_grade_status_count_seq ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_count_max_3,
        row_count - LAG(row_count) OVER w_all_by_n_grade_status_not_in_2 AS row_count_change,
        DENSE_RANK() OVER w_n_grade_status_not_in_incomp_2 AS max_enrollment_grade_status_count_dense_rank,
        SUM(row_count) OVER w_enrollment_grade_status_coun AS enrollment_grade_status_count_seq_grade_status_s
    FROM summary
    WINDOW w_all_by_n_grade_status_not_in_2 AS (ORDER BY n_grade_status_not_in_incomple DESC, grade_status, enrollment_grade_status_count_seq), w_n_grade_status_not_in_incomp_2 AS (PARTITION BY n_grade_status_not_in_incomple, enrollment_grade_status_count_seq ORDER BY max_enrollment_grade_status_count), w_enrollment_grade_status_coun AS (PARTITION BY enrollment_grade_status_count_seq, grade_status)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed_2
    WHERE max_enrollment_grade_status_count_dense_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT max_enrollment_grade_status_count, grade_status, enrollment_grade_status_count_seq_grade_status_s
    FROM first_rows
) src
PIVOT (
    SUM(enrollment_grade_status_count_seq_grade_status_s) AS amount, COUNT(enrollment_grade_status_count_seq_grade_status_s) AS n
    FOR grade_status IN ('WITHDRAWN', 'FAIL', 'PASS')
)
