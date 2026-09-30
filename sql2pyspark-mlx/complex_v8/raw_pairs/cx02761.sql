WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        ABS(score - 44.4) AS score_distance
    FROM enrollments
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        SUM(score) AS total_enrollment_score,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.total_enrollment_score,
        e.first_enrollment_enrolled_at
    FROM prepared_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_first_enrollment_enrolled_at AS score_distance_dense_rank,
        LAST_VALUE(total_enrollment_score) OVER (PARTITION BY DATE_TRUNC('MONTH', first_enrollment_enrolled_at) ORDER BY score_distance NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_enrollment_score
    FROM summarized_rows
    WINDOW w_first_enrollment_enrolled_at AS (PARTITION BY DATE_TRUNC('MONTH', first_enrollment_enrolled_at) ORDER BY score_distance NULLS LAST)
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_distance,
        s.last_enrollment_enrolled_at,
        s.first_enrollment_enrolled_at,
        s.last_total_enrollment_score,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM windowed s
    JOIN enrollments e ON e.student_id = s.student_id
)
SELECT
    enrollment_term,
    student_id,
    enrollment_course_id,
    SUM(CASE WHEN enrollment_term NOT IN ('2025T2', '2024T1') THEN 1 ELSE 0 END) AS n_enrollment_term_not_in_2025t,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
FROM line_level
GROUP BY GROUPING SETS ((enrollment_term, student_id, enrollment_course_id), (enrollment_term), ())
ORDER BY enrollment_term NULLS LAST, student_id NULLS LAST, enrollment_course_id NULLS LAST
