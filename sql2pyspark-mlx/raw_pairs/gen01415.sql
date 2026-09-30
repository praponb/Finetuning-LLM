WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(amount, 0) amount_filled,
        COALESCE(channel, 'DIRECT') channel_clean
    FROM orders
    WHERE shipping_fee <= 35
      AND order_date >= DATE '2023-08-19'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM base
    EXCEPT ALL
    SELECT status
    FROM orders
    WHERE customer_id = 3
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.status = k.status
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY shipping_fee NULLS FIRST) shipping_fee_cume_dist,
        MAX(amount) OVER (PARTITION BY channel) channel_max_amount,
        MIN(shipping_fee) OVER (PARTITION BY customer_id) customer_id_min_shipping_fee
    FROM verified
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount_filled,
        channel_clean,
        shipping_fee_cume_dist,
        channel_max_amount,
        ROUND(shipping_fee * 1.07, 2) shipping_fee_scaled
    FROM metrics
    WHERE order_date >= DATE '2023-05-20'
      AND channel IS DISTINCT FROM 'store'
)
-- Grouped metrics
SELECT
    status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    APPROX_PERCENTILE(shipping_fee_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_cume_dist_quartiles,
    PERCENTILE_APPROX(amount_filled, 0.9) AS amount_filled_p90
FROM valid
GROUP BY status
ORDER BY amount_filled_p90;
