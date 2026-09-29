WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        discount + amount AS discount_plus_amount
    FROM orders
    WHERE order_date > '2023-06-17'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM ranked
    EXCEPT ALL
    SELECT status
    FROM orders
    WHERE discount < 0.36
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.status = k.status
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept_rows s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL '7' DAY
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price >= 340
        )
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, d.age
    FROM kept s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.credit_limit <= 13400
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        channel,
        discount,
        shipping_fee,
        discount_pct
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (discount, shipping_fee, discount_pct AS DISCOUNT_PCT)
)
