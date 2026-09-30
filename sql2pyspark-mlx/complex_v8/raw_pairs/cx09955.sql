WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE course_id IS NOT NULL
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM recent s
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at_hour,
        s.score_filled,
        s.first_enrollment_enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM profiled s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
)
SELECT
    student_id,
    enrolled_at_hour,
    enrollment_id,
    GROUPING_ID() AS gid,
    SUM(score) FILTER (WHERE enrollment_term IN ('2025T2', '2024T1')) AS score_enrollment_term_in_2025t2_20
FROM with_children
GROUP BY CUBE(student_id, enrolled_at_hour, enrollment_id)
ORDER BY student_id NULLS LAST, enrolled_at_hour NULLS LAST, enrollment_id NULLS LAST
