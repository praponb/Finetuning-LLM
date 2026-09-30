WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity NULLS LAST, id) AS latest_rank
    FROM inventory
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST) AS quantity_cume_dist,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY latest_rank DESC, id) AS row_seq
    FROM ranked
    WHERE latest_rank < 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        latest_rank,
        quantity_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) AS event_seq,
        FIRST_VALUE(row_seq, TRUE) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq
    FROM with_windows
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY latest_rank DESC, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    product_id,
    CASE WHEN first_known_row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS first_known_row_seq_tier,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(event_seq, 0.9) AS event_seq_p90
FROM deduped
GROUP BY ROLLUP (product_id, CASE WHEN first_known_row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT(*) >= 1;
