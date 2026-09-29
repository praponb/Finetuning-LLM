WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE channel IN ('partner', 'mobile', 'web') AND status = 'PENDING'
),
Agg AS (
    SELECT MONTH(order_date) AS order_month, MIN(order_date) AS first_order_date, MAX(order_date) AS latest_order_date
    FROM filtered_orders
    GROUP BY MONTH(order_date)
)
SELECT *
FROM Agg
ORDER BY order_month DESC, latest_order_date;
