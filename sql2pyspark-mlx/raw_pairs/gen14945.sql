WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_num
    FROM orders
    QUALIFY row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        row_num,
        LAG(amount, 1) OVER (PARTITION BY status ORDER BY shipping_fee, id) AS prev_amount,
        CUME_DIST() OVER w_recent AS discount_cume_dist,
        AVG(discount) OVER w_grp AS discount_avg_6
    FROM deduped
    WINDOW w_recent AS (PARTITION BY YEAR(order_date), status ORDER BY discount DESC NULLS FIRST), w_grp AS (ORDER BY row_num, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount_cume_dist, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
deduped_orders AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY rank_in_group DESC, id) AS rank_in_group_2
    FROM latest
    QUALIFY rank_in_group_2 = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND HOUR(c.paid_at) BETWEEN 12 AND 15
        )
)
-- Grouped metrics
SELECT
    store_id,
    CASE WHEN prev_amount >= 1460 THEN 'HIGH' ELSE 'LOW' END AS prev_amount_tier,
    APPROX_PERCENTILE(shipping_fee, 0.95) AS shipping_fee_p95,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count
FROM eligible
GROUP BY store_id, CASE WHEN prev_amount >= 1460 THEN 'HIGH' ELSE 'LOW' END
ORDER BY shipping_fee_p95
