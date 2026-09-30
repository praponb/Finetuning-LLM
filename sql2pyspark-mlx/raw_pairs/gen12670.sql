WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.coupon_codes,
        pos + 1 AS item_no,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts src
    LATERAL VIEW OUTER POSEXPLODE(src.items) li AS pos, elem
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.signup_date,
        d.age,
        d.country
    FROM lines src
    LEFT JOIN customers d
        ON src.customer_id = d.id
    WHERE d.name LIKE 'K%'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.country AS customer_country,
        d.name,
        d.signup_date AS customer_signup_date
    FROM joined a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM carts
    WHERE created_at >= '2024-04-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.id = k.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.coupon_codes,
        e.item_no,
        e.product_id,
        e.qty,
        e.promo,
        e.signup_date,
        e.country,
        e.customer_country,
        c.segment,
        c.name AS customer_name
    FROM reconciled e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
SELECT
    customer_id,
    item_no,
    product_id,
    promo,
    country,
    customer_country
FROM joined_carts
ORDER BY item_no DESC NULLS FIRST;
