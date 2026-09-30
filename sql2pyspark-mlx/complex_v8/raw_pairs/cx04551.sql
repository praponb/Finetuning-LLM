WITH valid AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        ABS(credits - 2) AS credits_distance,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM courses
    WHERE level <> 'intro'
      AND credits >= 2
),
level_stats AS (
    -- Avg credits_distance per level
    SELECT level, AVG(credits_distance) AS avg_credits_distance
    FROM valid
    GROUP BY level
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_credits_distance,
        ROUND(s.credits_distance - g.avg_credits_distance, 2) AS credits_distance_minus_avg
    FROM valid s
    JOIN level_stats g ON g.level = s.level
    WHERE s.credits_distance > g.avg_credits_distance
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        dept,
        credits,
        level,
        credits_distance,
        dept_slug,
        avg_credits_distance,
        MIN(credits_distance) OVER (ORDER BY avg_credits_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_credits_distance_so_far,
        COUNT(*) OVER (PARTITION BY credits_distance, credits) AS rows_in_credits_distance_credits
    FROM outliers
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.student_id <= 17
        )
)
SELECT
    COALESCE(dept, 'ALL') AS dept,
    credits,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE level = 'advanced') AS n_level_advanced,
    COUNT(*) AS cnt
FROM filtered
GROUP BY GROUPING SETS ((dept, credits), (dept), (credits), ())
