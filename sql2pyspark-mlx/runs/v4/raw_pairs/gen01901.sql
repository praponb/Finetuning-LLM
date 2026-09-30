WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(cost) OVER (PARTITION BY order_id) AS order_id_max_cost,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        ROW_NUMBER() OVER (ORDER BY delivered_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS seq_no,
        LAST_VALUE(carrier, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier
    FROM shipments
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    carrier,
    SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) AS delivered_cost,
    AVG(last_known_cost) FILTER (WHERE status = 'RETURNED') AS returned_avg_last_known_cost,
    MAX(seq_no) FILTER (WHERE carrier = 'UPS') AS ups_max_seq_no,
    ROUND(AVG(last_known_cost), 2) AS avg_last_known_cost,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50,
    ROUND(AVG(last_known_cost), 2) AS avg_last_known_cost_2
FROM metrics
GROUP BY 1, 2
ORDER BY carrier DESC;
