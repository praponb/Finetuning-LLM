WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value
),
joined AS (
    SELECT src.*, d.email
    FROM flattened src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
joined_carts AS (
    SELECT
        e.*,
        b.age,
        b.segment,
        b.signup_date
    FROM joined e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
SELECT *
FROM (
    SELECT
        attr_key,
        segment,
        age
    FROM joined_carts
) src
PIVOT (
    MAX(age) AS total,
    COUNT(1) AS n
    FOR segment IN ('SMB', 'Corporate')
);
