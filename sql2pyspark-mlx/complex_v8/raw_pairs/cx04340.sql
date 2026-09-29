WITH scoped_courses AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM courses
    WHERE level IN ('core', 'advanced')
      AND LENGTH(TRIM(course_code)) >= 5
),
deduped AS (
    -- Keep the latest row per level
    SELECT
        id,
        dept,
        credits,
        level,
        credits_filled,
        level_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level ORDER BY credits DESC, id DESC) AS rn
        FROM scoped_courses
    ) ranked
    WHERE rn = 1
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM deduped s
    JOIN enrollments e ON e.course_id = s.id
),
enrollment_grade_status_stats AS (
    -- Avg enrollment_score per enrollment_grade_status
    SELECT enrollment_grade_status, AVG(enrollment_score) AS avg_enrollment_score
    FROM with_children
    GROUP BY enrollment_grade_status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_score,
        ROUND(s.enrollment_score - g.avg_enrollment_score, 2) AS enrollment_score_minus_avg
    FROM with_children s
    JOIN enrollment_grade_status_stats g ON g.enrollment_grade_status = s.enrollment_grade_status
    WHERE s.enrollment_score > g.avg_enrollment_score
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.id) AS min_enrollment_score
    FROM above_average s
)
SELECT
    id,
    enrollment_grade_status,
    avg_enrollment_score,
    enrollment_score_minus_avg,
    min_enrollment_score,
    CASE WHEN avg_enrollment_score >= 70.0 THEN 'HIGH' WHEN avg_enrollment_score >= 30.0 THEN 'MEDIUM' ELSE 'LOW' END AS avg_enrollment_score_tier
FROM with_counts
WHERE enrollment_score_minus_avg BETWEEN -70.0 AND 30.0
ORDER BY avg_enrollment_score_tier
