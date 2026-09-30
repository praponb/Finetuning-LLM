WITH flattened AS (
    SELECT
        t.id,
        t.customer_id,
        t.coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts t
    LATERAL VIEW OUTER EXPLODE(t.items) li AS elem
),
exploded AS (
    SELECT
        customer_id,
        product_id,
        price,
        value
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS value
),
matched_keys AS (
    SELECT customer_id
    FROM exploded
    INTERSECT ALL
    SELECT customer_id
    FROM carts
    WHERE customer_id IN (11, 19)
),
kept_rows AS (
    SELECT s.*
    FROM exploded s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
filtered AS (
    SELECT *, ROUND(price / product_id, 2) AS price_per_product_id
    FROM kept_rows
    WHERE value IS DISTINCT FROM 'BUNDLE'
),
cleaned AS (
    SELECT
        customer_id,
        product_id,
        price,
        value,
        product_id - price_per_product_id AS product_id_minus_price_per_product_id,
        ROUND(price_per_product_id / product_id, 2) AS price_per_product_id_per_product_id
    FROM filtered
    WHERE price_per_product_id >= 75
),
enriched AS (
    SELECT
        src.customer_id,
        src.price,
        src.price_per_product_id_per_product_id,
        b.city,
        b.age,
        b.email
    FROM cleaned src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.city IN ('Tokyo', 'Bangkok', 'Singapore')
),
joined AS (
    SELECT
        e.*,
        c.country,
        c.credit_limit
    FROM enriched e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
SELECT *
FROM (
    SELECT
        country,
        customer_id,
        city,
        price
    FROM joined
) src
PIVOT (
    SUM(price) AS amount,
    COUNT(1) AS n
    FOR city IN ('Chiang Mai' AS chiang_mai, 'Singapore' AS singapore)
)
