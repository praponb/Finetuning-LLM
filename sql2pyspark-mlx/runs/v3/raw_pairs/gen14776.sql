WITH FilteredShipments AS (
    SELECT *
    FROM shipments
    WHERE order_id IN (17, 28, 33) AND shipped_date <= '2023-09-01' AND shipped_date < '2025-08-14'
),
shipments_stats AS (
    SELECT
        YEAR(delivered_date) delivered_year,
        YEAR(shipped_date) shipped_year,
        ROUND(AVG(cost), 2) avg_cost,
        SORT_ARRAY(COLLECT_SET(status)) status_list
    FROM FilteredShipments
    GROUP BY YEAR(delivered_date), YEAR(shipped_date)
)
SELECT *
FROM shipments_stats
WHERE avg_cost < 44
ORDER BY delivered_year DESC
