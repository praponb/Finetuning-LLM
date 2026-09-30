WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365.25 AS years_since_last_restock
    FROM inventory
    WHERE store_id IN (1, 3, 4, 5)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        years_since_last_restock,
        SUM(years_since_last_restock) OVER (PARTITION BY DATE_TRUNC('month', last_restock), product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_years_since_last_restock,
        LEAD(years_since_last_restock, 1, 0) OVER (PARTITION BY product_id ORDER BY years_since_last_restock, id) AS next_years_since_last_restock
    FROM cleaned
    WHERE product_id IN (2, 5, 8, 10)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(years_since_last_restock) OVER w_hist AS prev_years_since_last_restock,
        LEAD(years_since_last_restock) OVER w_hist AS next_years_since_last_restock_2,
        LAG(running_years_since_last_restock) OVER w_hist AS prev_running_years_since_last_restock,
        LEAD(running_years_since_last_restock) OVER w_hist AS next_running_years_since_last_restock,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM scored
    WINDOW w_hist AS (ORDER BY next_years_since_last_restock, id)
),
scored_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        running_years_since_last_restock,
        prev_years_since_last_restock,
        next_years_since_last_restock_2,
        prev_running_years_since_last_restock,
        next_running_years_since_last_restock,
        row_seq,
        NTILE(5) OVER (ORDER BY next_years_since_last_restock_2, id) AS next_years_since_last_restock_2_bucket,
        FIRST_VALUE(next_running_years_since_last_restock) IGNORE NULLS OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_running_years_since_last_restock,
        LEAD(running_years_since_last_restock, 1) OVER (ORDER BY prev_running_years_since_last_restock, id) AS next_running_years_since_last_restock_2,
        AVG(next_years_since_last_restock_2) OVER (ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_years_since_last_restock_2_avg_5_prior,
        ROW_NUMBER() OVER (ORDER BY row_seq, id) AS row_seq_2
    FROM metrics
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(row_seq_2, 0) AS row_seq_2_filled
    FROM scored_inventory
    WHERE row_seq_2 BETWEEN 6 AND 7
),
base_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, next_running_years_since_last_restock_2 * next_running_years_since_last_restock AS next_running_years_since_last_restock_2_times_next_running_years_since_last_restock
    FROM base
    WHERE next_years_since_last_restock_2 BETWEEN 63 AND 68
)
SELECT
    id,
    running_years_since_last_restock,
    prev_years_since_last_restock,
    next_years_since_last_restock_2,
    next_running_years_since_last_restock,
    row_seq,
    next_years_since_last_restock_2_bucket,
    next_running_years_since_last_restock_2,
    row_seq_2
FROM base_inventory
