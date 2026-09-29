WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        NTILE(4) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        LAG(cost, 2) OVER (ORDER BY cost, id) AS prev_cost
    FROM shipments
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cost_bucket) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_bucket,
        LAG(prev_cost, 2) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_prev_cost,
        FIRST_VALUE(cost_bucket, TRUE) OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_bucket,
        COUNT(cost) OVER (PARTITION BY order_id) AS order_id_count_cost
    FROM metrics
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, o.channel
    FROM scored s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, carrier ORDER BY first_known_cost_bucket) AS dedup_rank
        FROM joined
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, o.discount
    FROM latest s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER w_running AS running_cost,
        NTILE(5) OVER (PARTITION BY carrier ORDER BY first_known_cost_bucket DESC, id) AS first_known_cost_bucket_bucket,
        AVG(discount) OVER (ORDER BY prev_prev_cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6
    FROM enriched
    WINDOW w_running AS (PARTITION BY carrier, order_id ORDER BY first_known_cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    status,
    AVG(prev_cost) FILTER (WHERE carrier = 'FedEx') AS fedex_avg_prev_cost,
    MAX(order_id_count_cost) FILTER (WHERE carrier = 'FedEx') AS fedex_max_order_id_count_cost,
    ROUND(AVG(order_id_count_cost), 2) AS avg_order_id_count_cost,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    ROUND(AVG(prev_prev_cost), 2) AS avg_prev_prev_cost,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count_2
FROM enriched_shipments
GROUP BY 1
ORDER BY avg_prev_prev_cost DESC NULLS LAST;
