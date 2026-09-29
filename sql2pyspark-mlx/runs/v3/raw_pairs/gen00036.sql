WITH src AS (
    SELECT *, LAST_DAY(delivered_date) AS delivered_date_month_end
    FROM shipments
    WHERE carrier IN ('FedEx', 'Kerry', 'UPS') OR shipped_date BETWEEN '2023-04-06' AND '2023-12-01'
),
shipments_stats AS (
    SELECT order_id, ROUND(AVG(cost), 2) AS avg_cost
    FROM src
    GROUP BY order_id
)
SELECT order_id, avg_cost
FROM shipments_stats;
