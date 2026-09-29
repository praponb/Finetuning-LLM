WITH joined AS (
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.paid_at,
        s.status,
        o.amount AS order_amount,
        o.channel,
        o.order_date
    FROM payments s
    LEFT JOIN orders o
        ON s.order_id <=> o.id
    WHERE o.status IN ('COMPLETED', 'SHIPPED', 'RETURNED')
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY channel ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_method,
        COUNT(order_amount) OVER w_grp order_amount_count_30d,
        LAST_VALUE(method) IGNORE NULLS OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_method,
        LAST_VALUE(method, TRUE) OVER w_ordered last_known_method_2,
        SUM(order_amount) OVER (PARTITION BY status, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_order_amount
    FROM joined
    WINDOW w_grp AS (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY MONTH(order_date), status ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM metrics
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    SELECT
        id,
        method,
        paid_at,
        status,
        order_amount,
        first_known_method,
        last_known_method_2,
        SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY YEAR(paid_at), status ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_method,
        SUM(session_id) OVER (PARTITION BY TO_DATE(paid_at), status ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_session_id,
        SUM(session_id) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) session_id_sum_30d
    FROM islands
)
SELECT
    CASE WHEN order_amount >= 1760 THEN 'LARGE' ELSE 'SMALL' END AS order_amount_tier,
    CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    PERCENTILE_APPROX(session_id_sum_30d, ARRAY(0.25, 0.5, 0.75)) AS session_id_sum_30d_quartiles,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    APPROX_PERCENTILE(session_id, 0.5) AS session_id_p50,
    GROUPING(CASE WHEN order_amount >= 1760 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM enriched
GROUP BY CUBE (CASE WHEN order_amount >= 1760 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END)
