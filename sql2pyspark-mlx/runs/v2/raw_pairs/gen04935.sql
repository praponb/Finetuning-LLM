WITH base AS (
    SELECT *
    FROM shipments
    WHERE shipped_date BETWEEN '2023-10-23' AND '2024-05-01' AND delivered_date BETWEEN '2023-11-28' AND '2025-07-01'
),
summary AS (
    SELECT
        MONTH(delivered_date) AS delivered_month,
        carrier,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values,
        AVG(cost) AS avg_cost
    FROM base
    GROUP BY MONTH(delivered_date), carrier
)
SELECT
    delivered_month,
    carrier,
    carrier_values,
    avg_cost
FROM summary
WHERE avg_cost < 89
ORDER BY carrier_values
