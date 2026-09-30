WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS FIRST, id) AS dedup_rank
        FROM inventory
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, p.price
    FROM deduped e
    LEFT JOIN products p
        ON e.product_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS row_seq,
        LAST_VALUE(price) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price
    FROM enriched
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity NULLS LAST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS event_seq,
        AVG(last_known_price) OVER (PARTITION BY store_id ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last_known_price_avg_2
    FROM latest
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM with_windows
    EXCEPT ALL
    SELECT store_id, id
    FROM inventory
    WHERE last_restock < '2025-09-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    GROUPING(store_id) AS grouping_level
FROM kept_rows
GROUP BY ROLLUP (store_id)
