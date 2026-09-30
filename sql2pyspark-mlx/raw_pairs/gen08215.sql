WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(shipping_fee) OVER (PARTITION BY status, channel ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY status, channel ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_shipping_fee,
        MIN(shipping_fee) OVER (PARTITION BY status, channel ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_shipping_fee,
        LAG(amount, 1) OVER (ORDER BY shipping_fee, id) AS prev_amount
    FROM orders
    WHERE customer_id IN (1, 17, 21)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        order_date,
        shipping_fee,
        last3_avg_shipping_fee,
        last3_sum_shipping_fee,
        last3_min_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY last3_sum_shipping_fee, id) AS row_seq,
        LAST_VALUE(prev_amount, TRUE) OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_amount
    FROM metrics
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.sqft,
        p.city,
        p.region
    FROM enriched s
    JOIN stores p
        ON s.store_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        order_date,
        last3_avg_shipping_fee,
        last3_sum_shipping_fee,
        row_seq,
        sqft,
        city,
        region,
        AVG(last3_min_shipping_fee) OVER (ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last3_min_shipping_fee_avg_3_prior,
        PERCENT_RANK() OVER (ORDER BY row_seq NULLS FIRST) AS row_seq_percent_rank,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY last_known_prev_amount DESC NULLS LAST) AS last_known_prev_amount_dense_rank
    FROM with_dims
)
SELECT
    id,
    last3_avg_shipping_fee,
    last3_sum_shipping_fee,
    row_seq,
    sqft,
    city,
    region,
    row_seq_percent_rank
FROM with_windows
ORDER BY city;
