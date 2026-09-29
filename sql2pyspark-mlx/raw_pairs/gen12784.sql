WITH valid AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE quantity <= 227
),
with_windows AS (
    SELECT
        id,
        quantity,
        quantity_filled,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity_filled DESC, id) AS quantity_filled_bucket,
        DENSE_RANK() OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY quantity_filled NULLS LAST) AS quantity_filled_dense_rank
    FROM valid
),
enriched AS (
    SELECT
        id,
        quantity,
        quantity_filled,
        quantity_filled_bucket,
        quantity_filled_dense_rank,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS event_seq,
        NTILE(5) OVER (ORDER BY quantity_filled, id) AS quantity_filled_bucket_2
    FROM with_windows
    WHERE quantity_filled_dense_rank <= 3
),
filtered AS (
    SELECT
        *,
        ROUND(quantity_filled_bucket * 1.15, 2) AS quantity_filled_bucket_scaled,
        event_seq - quantity_filled_bucket AS event_seq_minus_quantity_filled_bucket
    FROM enriched
    WHERE quantity_filled_bucket >= 5
),
scored AS (
    SELECT
        id,
        quantity,
        event_seq_minus_quantity_filled_bucket,
        ROW_NUMBER() OVER (ORDER BY quantity_filled_bucket_scaled, id) AS seq_no,
        FIRST_VALUE(quantity_filled_bucket_2) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_filled_bucket_2
    FROM filtered
),
scored_inventory AS (
    SELECT
        *,
        AVG(event_seq_minus_quantity_filled_bucket) OVER w_running AS running_avg_event_seq_minus_quantity_filled_bucket,
        MAX(event_seq_minus_quantity_filled_bucket) OVER w_running AS running_max_event_seq_minus_quantity_filled_bucket,
        COUNT(event_seq_minus_quantity_filled_bucket) OVER w_running AS running_count_event_seq_minus_quantity_filled_bucket,
        ROW_NUMBER() OVER w_part AS seq_no_2
    FROM scored
    WINDOW w_running AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY quantity, id)
)
SELECT
    seq_no,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(first_known_quantity_filled_bucket_2, 0.95) AS first_known_quantity_filled_bucket_2_p95,
    BIT_OR(quantity) AS quantity_bit_or,
    APPROX_PERCENTILE(running_avg_event_seq_minus_quantity_filled_bucket, ARRAY(0.25, 0.5, 0.75)) AS running_avg_event_seq_minus_quantity_filled_bucket_quartiles,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored_inventory
GROUP BY seq_no
ORDER BY first_known_quantity_filled_bucket_2_p95 DESC;
