WITH valid_shipments AS (
    SELECT *, CASE WHEN cost >= 95 THEN 'High' WHEN cost >= 44 THEN 'Medium' ELSE 'Low' END AS cost_band
    FROM shipments
    WHERE shipped_date > '2024-05-01' AND (MONTH(shipped_date) IN (1, 5, 8) OR carrier = 'FedEx')
),
status_summary AS (
    SELECT status, MIN(delivered_date) AS first_delivered_date, MIN(delivered_date) AS min_delivered_date
    FROM valid_shipments
    GROUP BY status
)
SELECT status, min_delivered_date
FROM status_summary
ORDER BY min_delivered_date DESC
