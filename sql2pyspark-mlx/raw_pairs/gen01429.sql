WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE delivered_date < '2023-07-01' AND shipped_date > '2024-04-01' AND carrier NOT IN ('UPS', 'Flash')
),
status_summary AS (
    SELECT status, DATE_TRUNC('MONTH', shipped_date) AS shipped_month, COUNT(CASE WHEN status = 'LOST' THEN 1 END) AS lost_count
    FROM valid_shipments
    GROUP BY status, DATE_TRUNC('MONTH', shipped_date)
)
SELECT status, shipped_month, lost_count
FROM status_summary;
