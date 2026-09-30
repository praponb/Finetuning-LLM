WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS row_seq,
        LAG(cost, 1) OVER (PARTITION BY YEAR(delivered_date), carrier ORDER BY delivered_date, id) AS prev_cost
    FROM shipments
),
with_dims AS (
    SELECT
        e.id,
        e.order_id,
        e.delivered_date,
        e.cost,
        b.status AS order_status,
        b.shipping_fee,
        b.channel
    FROM scored e
    INNER JOIN orders b
        ON e.order_id <=> b.id
),
metrics AS (
    SELECT
        id,
        cost,
        order_status,
        channel,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY shipping_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2,
        LEAD(shipping_fee, 1) OVER w_hist AS next_shipping_fee,
        NTILE(3) OVER w_running AS shipping_fee_bucket,
        LAG(cost, 1) OVER w_hist AS prev_cost,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM with_dims
    WINDOW w_hist AS (ORDER BY cost, id), w_running AS (ORDER BY shipping_fee, id)
),
with_windows AS (
    SELECT
        id,
        cost,
        cost_avg_2,
        shipping_fee_bucket,
        running_cost,
        SUM(next_shipping_fee) OVER (PARTITION BY channel ORDER BY next_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_shipping_fee,
        MIN(next_shipping_fee) OVER (PARTITION BY channel ORDER BY next_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_shipping_fee,
        AVG(next_shipping_fee) OVER (PARTITION BY channel ORDER BY next_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_next_shipping_fee,
        RANK() OVER (PARTITION BY order_status, channel ORDER BY next_shipping_fee DESC NULLS LAST) AS next_shipping_fee_rank
    FROM metrics
),
stats AS (
    SELECT
        CASE WHEN running_sum_next_shipping_fee >= 172 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_next_shipping_fee_tier,
        CASE WHEN running_min_next_shipping_fee >= 44 THEN 'HIGH' ELSE 'LOW' END AS running_min_next_shipping_fee_tier,
        ROUND(AVG(running_min_next_shipping_fee), 2) AS avg_running_min_next_shipping_fee,
        APPROX_PERCENTILE(next_shipping_fee_rank, 0.5) AS next_shipping_fee_rank_p50
    FROM with_windows
    GROUP BY GROUPING SETS ((CASE WHEN running_sum_next_shipping_fee >= 172 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_min_next_shipping_fee >= 44 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN running_sum_next_shipping_fee >= 172 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT running_min_next_shipping_fee_tier, next_shipping_fee_rank_p50
FROM stats
ORDER BY running_min_next_shipping_fee_tier DESC NULLS LAST;
