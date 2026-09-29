WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, amount + discount AS amount_plus_discount
    FROM orders
    WHERE order_date > DATE '2023-08-04'
      AND discount > 0.27
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, status ORDER BY amount, id) = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM ranked src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date >= s.order_date
        )
),
not_flagged_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.unit_price <= 130
        )
)
SELECT
    id,
    order_date,
    status,
    channel,
    amount,
    shipping_fee,
    promo_id,
    promo_code,
    discount_pct
FROM not_flagged_orders
ORDER BY shipping_fee NULLS FIRST, status;
