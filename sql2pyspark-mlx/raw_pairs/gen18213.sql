WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM orders
    WHERE order_date > DATE '2023-03-13'
      AND channel IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.shipping_fee,
        s.status_title,
        d.signup_date,
        d.email,
        d.credit_limit
    FROM valid s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.segment IN ('Corporate', 'SMB', 'Consumer')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        order_date,
        shipping_fee,
        email,
        FIRST_VALUE(credit_limit, TRUE) OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY customer_id, status ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit_2,
        LEAD(shipping_fee, 2) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) AS next_shipping_fee
    FROM joined
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(shipping_fee) AS shipping_fee_whole
    FROM scored
    WHERE order_date >= TO_DATE('2023-08-17')
      AND customer_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.shipping_fee,
        s.email,
        s.next_shipping_fee,
        s.shipping_fee_whole,
        c.age,
        c.credit_limit
    FROM cleaned s
    JOIN customers c
        ON s.customer_id = c.id
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.shipping_fee,
        s.email,
        s.age,
        top1.shipment_shipped_date
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid_orders AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        shipping_fee,
        email,
        age,
        shipment_shipped_date,
        age - shipping_fee AS age_minus_shipping_fee,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
    FROM with_last
    WHERE shipment_shipped_date >= '2023-09-01'
)
SELECT
    id,
    shipping_fee,
    email,
    age,
    shipment_shipped_date
FROM valid_orders
