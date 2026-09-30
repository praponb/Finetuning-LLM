WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS item
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        CAST(created_at AS DATE) AS created_at_day,
        CAST(created_at AS DATE) AS created_at_day_2
    FROM lines
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, d.age
    FROM cleaned src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(qty, 0) AS qty_rounded,
        CASE WHEN age >= 57 THEN 'High' WHEN age >= 30 THEN 'Medium' ELSE 'Low' END AS age_band
    FROM enriched
    WHERE product_id = 6
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.email,
        p.name
    FROM filtered src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        created_at_day,
        created_at_day_2,
        age,
        qty_rounded,
        age_band,
        email,
        name,
        MIN(product_id) OVER (PARTITION BY customer_id) AS customer_id_min_product_id,
        SUM(product_id) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_sum_30m
    FROM with_dims
)
SELECT
    s.customer_id,
    s.created_at,
    s.product_id,
    s.created_at_day,
    s.age,
    s.qty_rounded,
    s.email,
    s.name,
    s.customer_id_min_product_id,
    (SELECT SUM(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS sum_order_discount
FROM metrics s
ORDER BY age DESC NULLS LAST, qty_rounded DESC;
