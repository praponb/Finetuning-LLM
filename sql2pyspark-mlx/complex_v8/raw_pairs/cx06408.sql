WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        ABS(score - 59.7) AS score_distance
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_bucket,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM cleaned s
    JOIN enrollments e ON e.student_id = s.student_id
),
with_details AS (
    -- Attach enrollment attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_bucket,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_score,
        e.score AS enrollment_score_2
    FROM exploded_detail s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.term IN ('2024T1', '2025T2', '2024T2')
),
with_parent_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.*,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at_2,
        e.grade_status AS enrollment_grade_status
    FROM with_details s
    JOIN enrollments e ON e.id = s.enrollment_id
    WHERE e.term = '2025T2'
)
SELECT
    id,
    course_id,
    term,
    enrolled_at,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_score,
    enrollment_term,
    enrollment_grade_status
FROM with_parent_enrollment
ORDER BY enrollment_grade_status
