WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level IS NOT NULL
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS event_seq,
        COUNT(budget) OVER (PARTITION BY location, dept_name) AS location_count_budget
    FROM kept
    WHERE location IN ('Tokyo', 'Berlin')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(location_count_budget) OVER (PARTITION BY dept_name, location ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_location_count_budget,
        MIN(location_count_budget) OVER (PARTITION BY dept_name, location ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_location_count_budget,
        MAX(location_count_budget) OVER (PARTITION BY dept_name, location ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_location_count_budget,
        COUNT(location_count_budget) OVER (PARTITION BY dept_name, location ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_location_count_budget,
        NTILE(5) OVER (PARTITION BY dept_name, location ORDER BY prev_budget, id) AS prev_budget_bucket
    FROM enriched
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        location,
        prev_budget,
        event_seq,
        location_count_budget,
        last7_sum_location_count_budget,
        last7_count_location_count_budget,
        prev_budget_bucket
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY prev_budget DESC, id) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        prev_budget
    FROM latest
) src
PIVOT (
    SUM(prev_budget)
    FOR dept_name IN ('Sales', 'HR', 'Marketing')
);
