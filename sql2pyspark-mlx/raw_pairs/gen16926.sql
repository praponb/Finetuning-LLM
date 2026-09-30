WITH flattened AS (
    SELECT
        t.id,
        t.customer_id,
        rec.product_id,
        rec.price
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) li AS rec
),
cleaned AS (
    SELECT
        *,
        CASE WHEN product_id >= 10 THEN 'High' WHEN product_id >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_band,
        ROUND(product_id * 0.85, 2) AS adjusted_product_id
    FROM flattened
    WHERE price >= 90
),
with_dims AS (
    SELECT a.*, d.country
    FROM cleaned a
    JOIN customers d
        ON a.customer_id = d.id
),
with_dims_carts AS (
    SELECT
        src.customer_id,
        src.product_id,
        src.adjusted_product_id,
        src.country,
        c.signup_date,
        c.country AS customer_country,
        c.segment
    FROM with_dims src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
SELECT
    customer_id,
    product_id,
    adjusted_product_id,
    country,
    signup_date,
    customer_country,
    segment
FROM with_dims_carts
ORDER BY customer_id DESC, product_id ASC;
