WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        amount * discount amount_times_discount,
        COALESCE(channel, 'NONE') channel_filled
    FROM orders
    WHERE order_date >= DATE '2023-09-15'
),
matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM valid e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date >= sub.start_date
       AND e.order_date <= DATE_ADD(sub.end_date, 3)
),
matched_orders AS (
    SELECT
        src.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM matched src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date BETWEEN sub.start_date AND sub.end_date
),
valid_orders AS (
    SELECT
        *,
        monthly_fee - subscription_id monthly_fee_minus_subscription_id,
        COALESCE(subscription_plan, 'DIRECT') resolved_subscription_plan
    FROM matched_orders
    WHERE channel IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        discount,
        shipping_fee,
        subscription_id,
        plan,
        subscription_id_2,
        subscription_monthly_fee,
        NTILE(5) OVER (PARTITION BY subscription_plan, channel ORDER BY discount, id, subscription_id, subscription_id_2) discount_bucket,
        AVG(discount) OVER (PARTITION BY plan ORDER BY amount_times_discount, id, subscription_id, subscription_id_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_avg_2,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY discount DESC, id, subscription_id, subscription_id_2) discount_bucket_2,
        SUM(shipping_fee) OVER (PARTITION BY YEAR(order_date), store_id) order_date_year_sum_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY subscription_monthly_fee, id, subscription_id, subscription_id_2) event_seq
    FROM valid_orders
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    discount,
    shipping_fee,
    subscription_id,
    subscription_id_2,
    subscription_monthly_fee,
    discount_avg_2,
    order_date_year_sum_shipping_fee,
    RANK() OVER (PARTITION BY plan ORDER BY discount_bucket DESC NULLS FIRST) discount_bucket_rank,
    ROW_NUMBER() OVER (ORDER BY discount_bucket DESC, id, subscription_id, subscription_id_2) seq_no
FROM with_windows;
