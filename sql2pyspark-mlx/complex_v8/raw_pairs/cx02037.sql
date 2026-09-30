WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id >= 11
      AND course_id >= 6
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term IS NOT NULL
        )
      AND s.grade_status IS NOT NULL
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(enrolled_at) AS enrolled_at_day,
        MIN(score) AS min_score,
        PERCENTILE_APPROX(score, 0.5) AS median_score,
        SUM(score) AS total_score,
        ROUND(STDDEV(score), 2) AS stddev_score
    FROM screened
    GROUP BY TO_DATE(enrolled_at)
),
screened_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE total_score > (SELECT MIN(total_score) FROM per_group)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', enrolled_at_day) AS enrolled_at_day_quarter,
        ROUND(stddev_score * 1.1, 2) AS stddev_score_adj,
        DATE_ADD(enrolled_at_day, 7) AS enrolled_at_day_plus_7d
    FROM screened_2
    WHERE stddev_score >= 40.0
),
screened_3 AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE total_score > (SELECT AVG(total_score) FROM tagged)
),
screened_4 AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened_3
    WHERE stddev_score_adj > (SELECT AVG(stddev_score_adj) FROM screened_3)
)
SELECT
    enrolled_at_day,
    min_score,
    median_score,
    total_score,
    stddev_score,
    enrolled_at_day_quarter,
    enrolled_at_day_plus_7d,
    ROUND(stddev_score_adj * 1.5, 2) AS stddev_score_adj_adj
FROM screened_4
WHERE YEAR(enrolled_at_day_plus_7d) = 2025
ORDER BY stddev_score
