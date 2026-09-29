WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS rn
    FROM shipments
    QUALIFY rn = 1
)
SELECT
    order_id,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    COUNT_IF(carrier = 'UPS') AS ups_events,
    ROUND(AVG(cost), 2) AS avg_cost
FROM deduped
GROUP BY CUBE (order_id);
