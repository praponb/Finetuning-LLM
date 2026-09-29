WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE (order_id IN (19, 20, 33) AND carrier = 'DHL') OR MONTH(shipped_date) IN (1, 5, 11)
),
Agg AS (
    SELECT MONTH(delivered_date) AS delivered_month, ROUND(AVG(cost), 2) AS avg_cost, COUNT(*) AS row_count
    FROM valid_shipments
    GROUP BY MONTH(delivered_date)
)
SELECT delivered_month, avg_cost, row_count
FROM Agg
ORDER BY delivered_month DESC
LIMIT 3;
