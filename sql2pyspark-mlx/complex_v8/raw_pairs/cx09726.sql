WITH recent AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT(course_code, '#', CAST(credits AS STRING)) AS course_code_ref
    FROM courses
    WHERE dept IN ('Bio', 'Math', 'Biz')
      AND level = 'advanced'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score
    FROM recent s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_counts
    WHERE credits > (SELECT AVG(credits) FROM with_counts)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level,
        course_code_ref,
        total_enrollment_score,
        CONCAT(course_code, '#', CAST(id AS STRING)) AS course_code_ref_2,
        CASE WHEN level IN ('advanced', 'intro') THEN 'group_a' WHEN level = 'core' THEN 'group_b' ELSE 'other' END AS level_group
    FROM with_activity
)
-- Subtotals with ROLLUP
SELECT
    level_group,
    credits,
    ROUND(STDDEV(credits), 2) AS stddev_credits
FROM derived
GROUP BY ROLLUP(level_group, credits)
ORDER BY level_group NULLS LAST, credits NULLS LAST
