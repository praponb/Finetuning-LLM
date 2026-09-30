WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.created_at,
        s.attr_key,
        b.signup_date
    FROM lines s
    LEFT JOIN customers b
        ON s.customer_id = b.id
    WHERE b.segment = 'SMB'
),
joined AS (
    SELECT
        s.*,
        d.city,
        d.credit_limit
    FROM with_dims s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
joined_carts AS (
    SELECT
        a.*,
        p.age,
        p.country
    FROM joined a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
SELECT
    c.segment,
    c.country AS customer_country,
    COUNT(*) AS rows_total,
    MAX(src.age) AS max_age
FROM joined_carts src
INNER JOIN customers c ON src.customer_id = c.id
GROUP BY GROUPING SETS ((c.segment, c.country), (c.segment), ());
