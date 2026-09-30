WITH filtered AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT_WS('-', course_code, dept) AS course_code_dept_label,
        LENGTH(TRIM(course_code)) AS course_code_len,
        CASE WHEN dept IN ('Bio', 'Biz') THEN 'group_a' WHEN dept = 'CS' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE LOWER(course_code) LIKE '%01%'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM filtered s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(min_enrollment_score / NULLIF(SUM(min_enrollment_score) OVER (PARTITION BY level, dept_group), 0), 4) AS min_enrollment_score_share_of_level_dept_group,
        COUNT(*) OVER (PARTITION BY level, dept_group) AS rows_in_level_dept_group
    FROM profiled
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.course_id = s.id) AS total_enrollment_score,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.id) AS min_enrollment_score_2
    FROM with_trends s
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
      AND s.min_enrollment_score_share_of_level_dept_group > 0.4
),
stats AS (
    -- Group-level totals
    SELECT
        credits,
        COUNT(*) FILTER (WHERE dept = 'Bio') AS n_dept_bio,
        SUM(CASE WHEN level <> 'intro' THEN 1 ELSE 0 END) AS n_level_intro,
        MAX(rows_in_level_dept_group) AS max_rows_in_level_dept_group,
        COUNT(*) AS n_rows,
        SUM(rows_in_level_dept_group) AS total_rows_in_level_dept_group,
        PERCENTILE_APPROX(course_code_len, 0.5) AS median_course_code_len
    FROM qualified
    GROUP BY 1
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE median_course_code_len < (SELECT MAX(median_course_code_len) FROM stats)
)
-- Final projection
SELECT
    credits,
    n_level_intro,
    max_rows_in_level_dept_group,
    n_rows,
    total_rows_in_level_dept_group,
    median_course_code_len,
    LEAST(n_dept_bio, n_rows) AS least_n_dept_bio_n_rows,
    COALESCE(n_dept_bio, 0) AS n_dept_bio_filled
FROM screened
ORDER BY total_rows_in_level_dept_group
