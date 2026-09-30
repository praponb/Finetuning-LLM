WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE course_id <= 11
      AND term <> '2025T2'
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_enrollments
    WHERE score_filled > (SELECT AVG(score_filled) FROM prepared_enrollments)
)
SELECT
    id,
    student_id,
    term_ref,
    score_filled,
    DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
FROM with_activity
