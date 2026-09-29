WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        LENGTH(location) AS location_length
    FROM departments
    WHERE dept_name = 'HR'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS LAST) AS budget_rank,
        LAG(budget, 2) OVER w_grp AS prev_budget
    FROM base
    WINDOW w_grp AS (ORDER BY location_length, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY prev_budget DESC NULLS LAST) AS prev_budget_dense_rank,
        LEAD(budget, 2) OVER (PARTITION BY location ORDER BY prev_budget, id) AS next_budget,
        SUM(prev_budget) OVER (PARTITION BY location) AS location_sum_prev_budget,
        ROW_NUMBER() OVER (ORDER BY prev_budget DESC, id) AS row_seq,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM metrics
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    location,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count,
    MIN(location_sum_prev_budget) AS min_location_sum_prev_budget,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows
FROM latest
GROUP BY CUBE (location)
HAVING COUNT(CASE WHEN location = 'Berlin' THEN 1 END) < 1;
