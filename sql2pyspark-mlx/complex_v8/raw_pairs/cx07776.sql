WITH filtered_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        credits,
        level,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM courses
    WHERE level = 'advanced'
      AND dept <> 'Biz'
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_courses
    WHERE credits > (SELECT AVG(credits) FROM filtered_courses)
),
filtered_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.grade_status = 'INCOMPLETE'
              AND e.score <= 79.3
        )
      AND s.credits BETWEEN 2 AND 3
)
SELECT
    course_code_len,
    credits,
    COALESCE(level, 'ALL') AS level,
    SUM(CASE WHEN level <> 'advanced' THEN 1 ELSE 0 END) AS n_level_advanced
FROM filtered_2
GROUP BY ROLLUP(course_code_len, credits, level)
ORDER BY course_code_len NULLS LAST, credits NULLS LAST, level NULLS LAST
