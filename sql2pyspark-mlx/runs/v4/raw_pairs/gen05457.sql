WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        FIRST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY cost, id) AS cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS seq_no
    FROM shipments
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    MAX(first_known_cost) FILTER (WHERE status = 'LOST') AS lost_max_first_known_cost,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count,
    STDDEV_SAMP(cost) AS cost_stddev,
    ROUND(AVG(cost_bucket), 2) AS avg_cost_bucket,
    COUNT(*) AS cnt
FROM scored
GROUP BY order_id
ORDER BY cost_stddev DESC NULLS FIRST;
