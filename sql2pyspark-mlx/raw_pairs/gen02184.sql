WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        COALESCE(region, 'unassigned') AS region_filled
    FROM stores
    WHERE region IS NOT DISTINCT FROM 'West'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft, id) AS seq_no,
        LAG(sqft, 2) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
        SUM(sqft) OVER (PARTITION BY YEAR(opened_date), region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS seq_no_2
    FROM base
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        store_name_normalized,
        prev_sqft,
        cumulative_sqft,
        seq_no + prev_sqft AS seq_no_plus_prev_sqft
    FROM metrics
    WHERE opened_date > DATE '2023-03-20'
      AND region IS NOT NULL
),
metrics_stores AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY sqft DESC) AS sqft_rank,
        LAST_VALUE(cumulative_sqft, TRUE) OVER w_hist AS last_known_cumulative_sqft
    FROM cleaned
    WHERE opened_date <= '2024-07-01'
    WINDOW w_hist AS (ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM metrics_stores
    WHERE opened_date >= DATE '2023-06-05'
      AND opened_date < '2025-06-01'
)
SELECT
    id,
    store_name,
    sqft,
    store_name_normalized,
    sqft_rank,
    last_known_cumulative_sqft,
    days_since_opened_date
FROM filtered
ORDER BY sqft_rank NULLS FIRST
