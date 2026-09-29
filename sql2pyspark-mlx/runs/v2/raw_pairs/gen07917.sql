WITH Src AS (
    SELECT *
    FROM orders
    WHERE customer_id = 31 AND (customer_id IN (3, 20, 21) OR shipping_fee < 29)
),
orders_stats AS (
    SELECT
        store_id,
        YEAR(order_date) AS order_year,
        AVG(discount) AS avg_discount,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM Src
    GROUP BY store_id, YEAR(order_date)
)
SELECT *
FROM orders_stats
WHERE avg_discount >= 0.38
ORDER BY avg_discount, order_year DESC, store_id
LIMIT 20
