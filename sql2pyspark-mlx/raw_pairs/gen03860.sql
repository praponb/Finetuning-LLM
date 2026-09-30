WITH filtered AS (
    SELECT
        *,
        LAST_DAY(order_date) AS order_date_month_end,
        COALESCE(discount, 0) AS discount_filled
    FROM orders
    WHERE order_date >= TO_DATE('2023-08-08')
),
enriched AS (
    SELECT
        id,
        customer_id,
        shipping_fee,
        discount_filled,
        SUM(amount) OVER (PARTITION BY status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY customer_id, store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        CORR(discount, amount) OVER (PARTITION BY store_id) AS corr_discount_amount
    FROM filtered
),
with_last AS (
    SELECT s.*, x.shipment_cost
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_dims AS (
    SELECT
        src.*,
        p.signup_date,
        p.city,
        p.segment
    FROM with_last src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.age <= 53
),
latest_match AS (
    SELECT s.*, top1.paid_at
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.signup_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN shipment_cost >= 75 THEN 'LARGE' ELSE 'SMALL' END AS shipment_cost_tier,
    APPROX_PERCENTILE(running_amount, ARRAY(0.25, 0.5, 0.75)) AS running_amount_quartiles,
    SUM(CASE WHEN city = 'New York' THEN corr_discount_amount ELSE 0 END) AS new_york_corr_discount_amount
FROM latest_match
GROUP BY CASE WHEN shipment_cost >= 75 THEN 'LARGE' ELSE 'SMALL' END
HAVING new_york_corr_discount_amount >= 0
ORDER BY new_york_corr_discount_amount DESC
