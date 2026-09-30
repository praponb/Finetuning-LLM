WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        order_date,
        status,
        discount,
        SUM(discount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        SUM(discount) OVER (PARTITION BY MONTH(order_date), channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        AVG(discount) OVER (PARTITION BY status ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_avg_6_prior
    FROM orders
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        order_date,
        cumulative_discount,
        discount_avg_6_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.product_id IS NULL
        )
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.shipment_shipped_date,
        x.cost
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date, c.cost
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    order_date,
    cumulative_discount,
    discount_avg_6_prior,
    shipment_shipped_date,
    cost
FROM with_last
ORDER BY cost NULLS FIRST;
