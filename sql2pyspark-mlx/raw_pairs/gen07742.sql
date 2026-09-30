WITH valid AS (
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM shipments
    WHERE MONTH(shipped_date) IN (2, 4, 5)
),
with_dims AS (
    SELECT
        s.*,
        o.order_date,
        o.channel,
        o.discount
    FROM valid s
    INNER JOIN orders o
        ON s.order_id <=> o.id
),
metrics AS (
    SELECT
        *,
        LAG(cost) OVER w_hist AS prev_cost,
        LEAD(cost) OVER w_hist AS next_cost,
        LAG(discount) OVER w_hist AS prev_discount,
        LEAD(discount) OVER w_hist AS next_discount,
        ROW_NUMBER() OVER w_hist AS seq_no,
        LAG(discount, 1) OVER (PARTITION BY status, order_id ORDER BY discount, id) AS prev_discount_2
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY channel ORDER BY shipped_date, id)
),
scored AS (
    SELECT
        id,
        delivered_date,
        status,
        carrier_filled,
        channel,
        prev_cost,
        prev_discount,
        next_discount,
        seq_no,
        prev_discount_2,
        COUNT(cost) OVER (PARTITION BY order_id, status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        SUM(cost) OVER (PARTITION BY order_id, status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        AVG(cost) OVER (PARTITION BY order_id, status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost
    FROM metrics
    WHERE seq_no <= 7
),
enriched AS (
    SELECT
        *,
        SUM(running_sum_cost) OVER (PARTITION BY status, channel ORDER BY running_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_cost,
        NTILE(3) OVER (PARTITION BY status ORDER BY prev_cost, id) AS prev_cost_bucket,
        SUM(running_count_cost) OVER (PARTITION BY DATE_TRUNC('month', delivered_date), status ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_count_cost,
        LEAD(prev_cost, 1) OVER (PARTITION BY channel ORDER BY prev_discount_2, id) AS next_prev_cost,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_count_cost, id) AS seq_no_2
    FROM scored
),
metrics_shipments AS (
    SELECT
        id,
        status,
        carrier_filled,
        channel,
        prev_discount,
        next_discount,
        seq_no,
        running_count_cost,
        seq_no_2,
        AVG(next_discount) OVER (PARTITION BY DATE_TRUNC('WEEK', delivered_date), channel ORDER BY running_avg_cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS next_discount_avg_3_prior,
        NTILE(3) OVER (PARTITION BY channel, status ORDER BY prev_discount_2, id) AS prev_discount_2_bucket,
        ROUND(next_prev_cost / SUM(next_prev_cost) OVER (PARTITION BY status), 4) AS next_prev_cost_share,
        FIRST_VALUE(running_count_cost, TRUE) OVER (PARTITION BY status ORDER BY next_prev_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_cost,
        ROUND(next_prev_cost / SUM(next_prev_cost) OVER (PARTITION BY channel), 4) AS next_prev_cost_share_2
    FROM enriched
)
SELECT
    status,
    CASE WHEN prev_discount >= 0.39 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_discount_tier,
    CASE WHEN running_count_cost >= 13 THEN 'LARGE' ELSE 'SMALL' END AS running_count_cost_tier,
    AVG(running_count_cost) AS avg_running_count_cost,
    ROUND(AVG(next_prev_cost_share_2), 2) AS avg_next_prev_cost_share_2,
    ROUND(SUM(running_count_cost) / NULLIF(COUNT(CASE WHEN status = 'LOST' THEN 1 END), 0), 2) AS running_count_cost_per_lost,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY AVG(running_count_cost) DESC) AS avg_running_count_cost_dense_rank
FROM metrics_shipments
GROUP BY status, prev_discount_tier, running_count_cost_tier
HAVING ROUND(AVG(next_prev_cost_share_2), 2) >= 0
ORDER BY running_count_cost_per_lost NULLS LAST;
