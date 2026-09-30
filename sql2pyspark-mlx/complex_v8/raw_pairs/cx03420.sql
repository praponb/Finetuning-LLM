WITH valid_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.id) AS min_enrollment_score
    FROM valid_courses s
),
unique_rows AS (
    -- Keep the latest row per level
    SELECT
        id,
        course_code,
        credits,
        level,
        min_enrollment_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level ORDER BY min_enrollment_score DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    course_code,
    level,
    min_enrollment_score,
    ROUND(min_enrollment_score * 0.9, 2) AS min_enrollment_score_adj
FROM unique_rows
ORDER BY course_code, id
LIMIT 20
