WITH enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.opened_date,
        d.sqft,
        d.store_name
    FROM orders e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        opened_date,
        sqft,
        store_name,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY sqft, id) AS sqft_bucket,
        LAG(amount, 1) OVER (PARTITION BY status ORDER BY shipping_fee, id) AS prev_amount,
        RANK() OVER (PARTITION BY customer_id, channel ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_rank
    FROM enriched
),
enriched_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY prev_amount) AS prev_amount_rank,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS event_seq,
        AVG(prev_amount) OVER (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev_amount_avg_2
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(prev_amount_rank, 1) AS prev_amount_rank_rounded,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM enriched_orders
    WHERE order_date > DATE '2023-04-29'
)
-- Grouped metrics
SELECT
    customer_id,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(prev_amount_avg_2, ARRAY(0.25, 0.5, 0.75)) AS prev_amount_avg_2_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(shipping_fee_rank) AS total_shipping_fee_rank,
    COUNT(*) AS cnt
FROM valid
GROUP BY customer_id, quarter_start
ORDER BY quarter_start DESC NULLS FIRST;
