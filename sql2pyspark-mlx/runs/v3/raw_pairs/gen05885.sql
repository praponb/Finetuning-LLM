WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE delivered_date < '2023-05-01'
),
shipments_stats AS (
    SELECT CASE WHEN cost >= 86 THEN 'high' WHEN cost >= 33 THEN 'mid' ELSE 'low' END AS cost_bucket, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM valid_shipments
    GROUP BY CASE WHEN cost >= 86 THEN 'high' WHEN cost >= 33 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM shipments_stats
