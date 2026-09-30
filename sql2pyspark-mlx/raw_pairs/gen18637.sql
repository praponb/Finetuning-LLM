WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CONCAT_WS('/', channel, status) channel_status_key,
        COALESCE(channel, 'N/A') channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-08-10'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(shipping_fee) OVER (PARTITION BY status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_shipping_fee,
        SUM(discount) OVER (PARTITION BY channel, status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount,
        SUM(shipping_fee) OVER (PARTITION BY customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_shipping_fee_2,
        NTILE(10) OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) shipping_fee_bucket
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        channel,
        amount,
        discount,
        channel_clean,
        cumulative_shipping_fee,
        running_discount,
        SUM(discount) OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_2,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY cumulative_shipping_fee_2 DESC, id) seq_no,
        AVG(amount) OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) amount_avg_3_prior
    FROM metrics
),
enriched_orders AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.city,
        b.segment
    FROM enriched a
    LEFT JOIN customers b
        ON a.customer_id = b.id
    WHERE b.is_active
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, p.region
    FROM enriched_orders s
    LEFT JOIN stores p
        ON s.store_id = p.id
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.channel,
    s.amount,
    s.discount,
    s.channel_clean,
    s.cumulative_shipping_fee,
    s.running_discount_2,
    (SELECT SUM(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_amount
FROM with_dims s
ORDER BY sum_order_amount
