WITH base AS (
    SELECT *, LEAST(amount, discount) AS least_amount_discount
    FROM orders
    WHERE store_id = 6
),
Agg AS (
    SELECT YEAR(order_date) AS order_year, MIN(order_date) AS first_order_date
    FROM base
    GROUP BY YEAR(order_date)
)
SELECT order_year, first_order_date
FROM Agg
ORDER BY order_year DESC
