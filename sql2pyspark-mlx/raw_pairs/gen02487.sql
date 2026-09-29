WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        shipping_fee,
        LENGTH(channel) AS channel_length,
        CASE WHEN discount >= 0.32 THEN 'High' WHEN discount >= 0.15 THEN 'Medium' ELSE 'Low' END AS discount_band
    FROM orders
    WHERE order_date >= DATE '2023-02-22'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date, id) AS latest_rank
        FROM cleaned
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        id,
        customer_id,
        shipping_fee,
        LEAD(shipping_fee, 2) OVER (PARTITION BY MONTH(order_date), store_id ORDER BY order_date, id) AS next_shipping_fee,
        LEAD(channel_length, 1) OVER (PARTITION BY store_id, customer_id ORDER BY channel_length, id) AS next_channel_length,
        MIN(shipping_fee) OVER (PARTITION BY customer_id) AS customer_id_min_shipping_fee,
        ROW_NUMBER() OVER (ORDER BY channel_length, id) AS row_seq
    FROM ranked
    WHERE customer_id IS NULL
),
enriched_orders AS (
    SELECT
        a.id,
        a.customer_id,
        a.shipping_fee,
        a.customer_id_min_shipping_fee,
        a.row_seq,
        c.country,
        c.name
    FROM enriched a
    LEFT JOIN customers c
        ON a.customer_id = c.id
)
SELECT
    s.id,
    s.customer_id,
    s.shipping_fee,
    s.customer_id_min_shipping_fee,
    s.row_seq,
    s.country,
    CASE WHEN country RLIKE '^(UK|JP)$' THEN 'GROUP_B' WHEN country RLIKE '^(TH|US)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS country_zone,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM enriched_orders s
ORDER BY max_subscription_monthly_fee DESC NULLS LAST
