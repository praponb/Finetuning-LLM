WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        item.product_id,
        item.qty,
        item.price
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, item
),
enriched AS (
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.coupon_codes,
        e.pos,
        e.product_id,
        e.qty,
        e.price,
        d.email,
        d.city
    FROM lines e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
    WHERE d.credit_limit > 5800
),
remaining_keys AS (
    SELECT customer_id, id
    FROM enriched
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
base AS (
    SELECT
        *,
        CASE WHEN city IN ('London', 'Chiang Mai') THEN 'Core' ELSE 'Other' END AS city_group,
        product_id + pos AS product_id_plus_pos
    FROM reconciled
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 10:00:00')
),
joined AS (
    SELECT
        a.*,
        c.age,
        c.credit_limit
    FROM base a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
with_dims AS (
    SELECT
        a.*,
        d.email AS customer_email,
        d.city AS customer_city
    FROM joined a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
enriched_carts AS (
    SELECT
        s.customer_id,
        s.coupon_codes,
        s.product_id,
        s.qty,
        s.city,
        s.city_group,
        s.product_id_plus_pos,
        s.age,
        s.customer_city,
        d.name
    FROM with_dims s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
)
SELECT
    product_id,
    qty,
    city,
    name
FROM enriched_carts
