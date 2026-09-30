WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount,
        discount,
        shipping_fee,
        DATE_ADD(order_date, 87) AS order_date_plus_87d,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM orders
    WHERE order_date >= TO_DATE('2023-05-05')
      AND store_id IS NOT NULL
      AND store_id IS NOT NULL
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.payment_paid_at
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.credit_limit,
        c.email
    FROM with_last src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount,
        discount,
        shipping_fee,
        order_date_plus_87d,
        status_title,
        channel_filled,
        payment_paid_at,
        credit_limit,
        email,
        LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY discount, id) AS next_amount,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount
    FROM joined
)
SELECT
    id,
    customer_id,
    order_date,
    amount,
    discount,
    shipping_fee,
    order_date_plus_87d,
    status_title,
    channel_filled
FROM metrics
ORDER BY customer_id DESC NULLS FIRST
