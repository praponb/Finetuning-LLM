WITH metrics AS (
    SELECT
        id,
        customer_id,
        channel,
        discount,
        LAG(amount) OVER (PARTITION BY customer_id, store_id ORDER BY discount, id) prev_amount,
        LEAD(amount) OVER (PARTITION BY customer_id, store_id ORDER BY discount, id) next_amount,
        LEAD(shipping_fee) OVER (PARTITION BY customer_id, store_id ORDER BY discount, id) next_shipping_fee,
        LAG(shipping_fee) OVER (PARTITION BY customer_id, store_id ORDER BY discount, id) prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY discount, id) seq_no,
        ROW_NUMBER() OVER (ORDER BY order_date DESC, id) row_seq
    FROM orders
    WHERE store_id IN (3, 5, 12, 23)
)
SELECT
    CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    channel,
    seq_no,
    SUM(prev_shipping_fee) FILTER (WHERE channel = 'web') AS web_total_prev_shipping_fee,
    SUM(CASE WHEN channel = 'store' THEN seq_no ELSE 0 END) AS store_seq_no,
    COUNT(*) AS row_count
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END, channel, seq_no), (CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END, channel), ())
HAVING SUM(CASE WHEN channel = 'store' THEN seq_no ELSE 0 END) > 0
ORDER BY row_count NULLS LAST
