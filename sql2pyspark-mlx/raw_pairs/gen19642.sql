WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), order_date) / 365.25 AS years_since_order_date,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE channel IS DISTINCT FROM 'web'
      AND order_date >= TO_DATE('2023-04-25')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS amount_rank,
        AVG(shipping_fee) OVER (PARTITION BY store_id, channel ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_3_prior
    FROM cleaned
    WHERE customer_id = 34
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.amount,
        s.discount,
        s.years_since_order_date,
        s.resolved_channel,
        s.shipping_fee_bucket,
        s.amount_rank,
        top1.unit_price,
        top1.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY order_date, id) AS row_num
        FROM latest_match
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        status,
        discount,
        resolved_channel,
        quantity,
        NTILE(3) OVER (ORDER BY discount, id) AS discount_bucket,
        NTILE(5) OVER (PARTITION BY status ORDER BY shipping_fee_bucket DESC, id) AS shipping_fee_bucket_bucket,
        AVG(years_since_order_date) OVER (ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS years_since_order_date_avg_6
    FROM deduped
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status,
        discount,
        resolved_channel,
        quantity,
        shipping_fee_bucket_bucket,
        LEAST(discount, discount_bucket) AS least_discount_discount_bucket
    FROM with_windows
    WHERE years_since_order_date_avg_6 BETWEEN 26 AND 75
),
cleaned_orders AS (
    -- Keep recent, valid rows
    SELECT *, shipping_fee_bucket_bucket - quantity AS shipping_fee_bucket_bucket_minus_quantity
    FROM filtered
    WHERE status IN ('SHIPPED', 'CANCELLED')
)
SELECT
    id,
    resolved_channel,
    shipping_fee_bucket_bucket
FROM cleaned_orders
ORDER BY id DESC NULLS FIRST
LIMIT 5
