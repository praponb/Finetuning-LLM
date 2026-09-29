WITH scored AS (
    SELECT
        *,
        LAG(unit_price) OVER w_recent AS prev_unit_price,
        LEAD(unit_price) OVER w_recent AS next_unit_price,
        LEAD(quantity) OVER w_recent AS next_quantity,
        ROW_NUMBER() OVER w_recent AS row_seq,
        NTILE(5) OVER w_running AS quantity_bucket
    FROM order_items
    WINDOW w_recent AS (PARTITION BY product_id, order_id ORDER BY quantity, id), w_running AS (PARTITION BY product_id ORDER BY quantity, id)
)
SELECT
    CASE WHEN quantity_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_bucket_tier,
    APPROX_PERCENTILE(quantity_bucket, 0.5) AS quantity_bucket_p50,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    AVG(prev_unit_price) AS avg_prev_unit_price,
    MIN(next_quantity) AS min_next_quantity
FROM scored
GROUP BY CASE WHEN quantity_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END
HAVING MIN(next_quantity) < 1000000;
