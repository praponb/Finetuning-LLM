WITH valid AS (
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM orders
    WHERE order_date >= DATE '2023-06-09'
      AND customer_id IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.unit_price < 350
        )
),
enriched AS (
    SELECT
        *,
        CORR(discount, shipping_fee) OVER w_part AS corr_discount_shipping_fee,
        LEAD(amount_int, 1) OVER (PARTITION BY customer_id, store_id ORDER BY amount, id) AS next_amount_int,
        CORR(shipping_fee, amount) OVER (PARTITION BY status) AS corr_shipping_fee_amount,
        STDDEV_SAMP(amount_int) OVER w_part_2 AS customer_id_stddev_samp_amount_int,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', order_date), status ORDER BY discount NULLS LAST) AS discount_dense_rank
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY store_id, status), w_part_2 AS (PARTITION BY customer_id)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_dense_rank NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
deduped_orders AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
summary AS (
    SELECT
        status,
        COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        SIZE(COLLECT_SET(channel)) AS num_channels,
        PERCENTILE_APPROX(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
        SUM(CASE WHEN channel = 'mobile' THEN shipping_fee ELSE 0 END) AS mobile_shipping_fee
    FROM deduped_orders
    GROUP BY status
)
SELECT
    status,
    distinct_customer_ids,
    num_channels,
    mobile_shipping_fee
FROM summary
ORDER BY mobile_shipping_fee DESC
