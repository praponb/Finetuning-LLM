WITH enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.shipping_fee,
        p.channel
    FROM shipments s
    INNER JOIN orders p
        ON s.order_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(shipping_fee) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_2,
        SUM(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        AVG(cost) OVER (PARTITION BY channel ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2
    FROM enriched
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, b.amount
    FROM with_windows src
    INNER JOIN orders b
        ON src.order_id = b.id
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(shipping_fee_avg_2) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee_avg_2,
        LAST_VALUE(carrier, TRUE) OVER (PARTITION BY order_id ORDER BY cost_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        CUME_DIST() OVER (ORDER BY shipping_fee_avg_2) AS shipping_fee_avg_2_cume_dist
    FROM joined
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER w_running AS shipping_fee_avg_2_cume_dist_bucket,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost NULLS FIRST) AS cost_dense_rank
    FROM enriched_shipments
    WINDOW w_running AS (PARTITION BY status ORDER BY shipping_fee_avg_2_cume_dist, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        shipping_fee,
        channel,
        shipping_fee_avg_2,
        cumulative_cost,
        cost_avg_2,
        amount,
        running_shipping_fee_avg_2,
        shipping_fee_avg_2_cume_dist,
        shipping_fee_avg_2_cume_dist_bucket,
        cost_dense_rank,
        LAST_DAY(shipped_date) AS shipped_date_month_end,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM metrics
    WHERE shipping_fee_avg_2_cume_dist_bucket != 7
)
-- Ranking and gap features
SELECT
    *,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY running_shipping_fee_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    AVG(cumulative_cost) OVER (PARTITION BY order_id ORDER BY cumulative_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cumulative_cost_avg_3,
    LEAD(shipping_fee, 1) OVER (PARTITION BY channel ORDER BY shipping_fee_avg_2, id) AS next_shipping_fee
FROM filtered;
