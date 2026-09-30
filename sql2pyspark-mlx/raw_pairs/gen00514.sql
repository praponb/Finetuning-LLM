WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock BETWEEN '2023-05-01' AND '2025-10-29'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_plus_months,
        LEAD(quantity, 2, 0) OVER (PARTITION BY store_id ORDER BY last_restock_plus_months, id) AS next_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        MAX(quantity) OVER w_ordered AS last_restock_plus_months_year_max_quantity
    FROM base
    WINDOW w_ordered AS (PARTITION BY YEAR(last_restock_plus_months), store_id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_plus_months_year_max_quantity DESC, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        next_quantity,
        running_quantity,
        last_restock_plus_months_year_max_quantity,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock_plus_months_year_max_quantity DESC NULLS LAST) AS last_restock_plus_months_year_max_quantity_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_quantity, id) AS event_seq
    FROM latest
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        next_quantity,
        last_restock_plus_months_year_max_quantity,
        last_restock_plus_months_year_max_quantity_dense_rank,
        event_seq,
        quantity * event_seq AS quantity_times_event_seq
    FROM enriched
    WHERE next_quantity = 81
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN quantity_times_event_seq >= 69 THEN 'LARGE' ELSE 'SMALL' END AS quantity_times_event_seq_tier,
    CASE WHEN event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(quantity_times_event_seq, ARRAY(0.25, 0.5, 0.75)) AS quantity_times_event_seq_quartiles
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN quantity_times_event_seq >= 69 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN quantity_times_event_seq >= 69 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY quantity_times_event_seq_tier DESC;
