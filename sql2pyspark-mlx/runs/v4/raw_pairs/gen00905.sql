WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(location) AS location_length,
        COALESCE(location, 'OTHER') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title = 'Analyst'
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_length,
        location_filled,
        LAG(location_length, 2) OVER (PARTITION BY dept_name ORDER BY budget, id) AS prev_location_length,
        AVG(location_length) OVER (PARTITION BY location ORDER BY location_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS location_length_avg_3,
        RANK() OVER (PARTITION BY location ORDER BY budget NULLS FIRST) AS budget_rank,
        NTILE(4) OVER (ORDER BY budget, id) AS budget_bucket
    FROM kept
)
SELECT
    id,
    budget,
    location_length,
    location_filled,
    location_length_avg_3,
    budget_bucket
FROM enriched
ORDER BY budget_bucket, location_length;
