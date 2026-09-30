WITH filtered_shipments AS (
    SELECT *
    FROM shipments
    WHERE shipped_date <= '2023-10-01' AND (shipped_date > '2023-07-01' OR shipped_date BETWEEN '2023-08-01' AND '2024-01-01')
),
shipments_stats AS (
    SELECT carrier, SUM(cost) AS total_cost
    FROM filtered_shipments
    GROUP BY carrier
)
SELECT carrier, total_cost
FROM shipments_stats
ORDER BY total_cost DESC, carrier DESC
