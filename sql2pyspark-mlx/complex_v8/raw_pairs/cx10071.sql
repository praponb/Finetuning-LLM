WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE grade_status <> 'INCOMPLETE'
      AND course_id IS NOT NULL
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        grade_status,
        ROUND(STDDEV(score_bucket), 2) AS stddev_score_bucket,
        SUM(score) AS total_score,
        MIN(score) AS min_score,
        ROUND(AVG(score_bucket), 2) AS avg_score_bucket
    FROM filtered_enrollments
    GROUP BY course_id, grade_status
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.course_id,
        s.grade_status,
        s.stddev_score_bucket,
        s.total_score,
        s.min_score,
        s.avg_score_bucket,
        c.course_code,
        c.level AS course_level
    FROM stats s
    JOIN courses c ON c.id = s.course_id
    WHERE c.level NOT IN ('advanced', 'intro')
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', grade_status, course_level) AS grade_status_course_level_label
    FROM combined_details
    WHERE course_id IS NOT NULL
),
course_level_stats AS (
    -- Avg stddev_score_bucket per course_level
    SELECT course_level, AVG(stddev_score_bucket) AS avg_stddev_score_bucket
    FROM derived
    GROUP BY course_level
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_stddev_score_bucket,
        ROUND(s.stddev_score_bucket - g.avg_stddev_score_bucket, 2) AS stddev_score_bucket_minus_avg
    FROM derived s
    JOIN course_level_stats g ON g.course_level = s.course_level
    WHERE s.stddev_score_bucket > g.avg_stddev_score_bucket
),
tagged AS (
    -- Add calculated columns
    SELECT
        course_id,
        grade_status,
        stddev_score_bucket,
        min_score,
        avg_score_bucket,
        course_code,
        course_level,
        CONCAT(course_code, '#', CAST(course_id AS STRING)) AS course_code_ref,
        CONCAT_WS('-', grade_status, course_level) AS grade_status_course_level_label_2,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM outliers
)
SELECT
    course_id,
    grade_status,
    stddev_score_bucket,
    min_score,
    avg_score_bucket,
    course_code_ref,
    grade_status_course_level_label_2,
    grade_status_ref
FROM tagged
ORDER BY grade_status DESC, stddev_score_bucket DESC, course_id
LIMIT 5
