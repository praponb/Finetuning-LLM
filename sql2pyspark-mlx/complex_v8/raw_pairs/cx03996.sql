WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 54.2) AS score_distance,
        COALESCE(score, 0.0) AS score_filled,
        ABS(score - 63.9) AS score_distance_2
    FROM enrollments
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM filtered s
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    score_distance,
    score_distance_2,
    DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour,
    DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
FROM with_activity
ORDER BY enrolled_at_year, score_distance DESC
