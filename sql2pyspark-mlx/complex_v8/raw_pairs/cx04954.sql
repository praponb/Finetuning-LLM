WITH filtered AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        SUBSTRING(TRIM(dept), 1, 3) AS dept_prefix
    FROM courses
    WHERE credits < 4
      AND course_code IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score,
        e.total_enrollment_score
    FROM filtered s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
dept_stats AS (
    -- Max total_enrollment_score per dept
    SELECT dept, MAX(total_enrollment_score) AS max_total_enrollment_score
    FROM with_stats
    GROUP BY dept
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_total_enrollment_score,
        ROUND(s.total_enrollment_score - g.max_total_enrollment_score, 2) AS total_enrollment_score_minus_max
    FROM with_stats s
    JOIN dept_stats g ON g.dept = s.dept
    WHERE s.total_enrollment_score = g.max_total_enrollment_score
),
level_stats AS (
    -- Max total_enrollment_score per level
    SELECT level, MAX(total_enrollment_score) AS max_total_enrollment_score_2
    FROM outliers
    GROUP BY level
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_total_enrollment_score_2,
        ROUND(s.total_enrollment_score - g.max_total_enrollment_score_2, 2) AS total_enrollment_score_minus_max_2
    FROM outliers s
    JOIN level_stats g ON g.level = s.level
    WHERE s.total_enrollment_score = g.max_total_enrollment_score_2
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM compared
    WHERE max_total_enrollment_score_2 > (SELECT AVG(max_total_enrollment_score_2) FROM compared)
)
-- Final projection
SELECT
    id,
    dept,
    credits,
    level,
    dept_prefix,
    avg_enrollment_score,
    total_enrollment_score,
    max_total_enrollment_score,
    total_enrollment_score_minus_max,
    max_total_enrollment_score_2,
    total_enrollment_score_minus_max_2,
    LENGTH(TRIM(level)) AS level_len
FROM with_activity
WHERE max_total_enrollment_score < 510.0
