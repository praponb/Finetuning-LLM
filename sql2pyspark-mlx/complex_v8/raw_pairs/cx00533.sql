WITH scoped_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT(course_code, '#', CAST(credits AS STRING)) AS course_code_ref,
        ABS(credits - 2) AS credits_distance,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 5
      AND credits BETWEEN 2 AND 4
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(course_code), 1, 2) AS course_code_prefix,
        ROUND(dept_len / NULLIF(credits_distance, 0), 3) AS dept_len_per_credits_distance,
        CASE WHEN dept IN ('Bio', 'Biz') THEN 'group_a' WHEN dept = 'CS' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM scoped_courses
    WHERE credits <= 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        dept_len - LAG(dept_len) OVER (PARTITION BY credits_distance, level ORDER BY dept_len_per_credits_distance DESC, id) AS dept_len_change,
        ROUND(credits / NULLIF(SUM(credits) OVER (PARTITION BY dept_len, dept), 0), 4) AS credits_share_of_dept_len_dept,
        ROUND(AVG(dept_len) OVER (PARTITION BY credits_distance, level ORDER BY dept_len_per_credits_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS dept_len_avg_7
    FROM labeled
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.score > 38.3
        )
),
filtered_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
      AND LOWER(s.course_code) LIKE '%05%'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score
    FROM filtered_2 s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
)
SELECT *
FROM (
    SELECT level, dept, credits_share_of_dept_len_dept
    FROM profiled
) src
PIVOT (
    SUM(credits_share_of_dept_len_dept)
    FOR dept IN ('CS', 'Math')
)
