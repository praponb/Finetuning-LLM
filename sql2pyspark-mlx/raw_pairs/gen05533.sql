WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE last_restock > DATE '2023-07-17'
      AND quantity BETWEEN 136 AND 239
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS FIRST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        last_restock
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS LAST, id) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM deduped
    WHERE YEAR(last_restock) >= 2023
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        ROW_NUMBER() OVER (ORDER BY last_restock DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', last_restock_month_end), store_id ORDER BY last_restock, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS event_seq_2
    FROM filtered
),
filtered_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(event_seq) AS event_seq_whole
    FROM metrics
    WHERE row_seq > 5
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    PERCENTILE_APPROX(row_seq, 0.9) AS row_seq_p90,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    SUM(event_seq_whole) AS total_event_seq_whole,
    COUNT(DISTINCT store_id) AS distinct_store_ids_2,
    AVG(row_seq) AS avg_row_seq
FROM filtered_inventory
GROUP BY store_id
HAVING total_event_seq_whole >= 0
ORDER BY total_event_seq_whole DESC;
