WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        discount,
        shipping_fee,
        ADD_MONTHS(order_date, 6) AS order_date_plus_months
    FROM orders
    WHERE order_date > DATE '2023-05-27'
      AND amount >= 1200
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date_plus_months BETWEEN sub.start_date AND sub.end_date
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.order_id IN (26, 35)
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM kept
    INTERSECT ALL
    SELECT customer_id, id
    FROM orders
    WHERE amount >= 1570
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
base_orders AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(shipping_fee, 0) AS shipping_fee_filled,
        COALESCE(plan, 'DIRECT') AS resolved_plan
    FROM kept_rows
    WHERE plan IS NOT NULL
      AND monthly_fee BETWEEN 70 AND 230
),
kept_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base_orders s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity > 6
        )
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.customer_id,
        src.store_id,
        src.order_date,
        src.discount,
        src.shipping_fee,
        src.order_date_plus_months,
        src.subscription_id,
        src.plan,
        src.resolved_plan,
        b.sqft
    FROM kept_orders src
    LEFT JOIN stores b
        ON src.store_id <=> b.id
    WHERE b.store_name LIKE 'Old%'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        customer_id,
        discount,
        shipping_fee
    FROM joined
) src
UNPIVOT (
    val FOR metric IN (discount, shipping_fee)
);
