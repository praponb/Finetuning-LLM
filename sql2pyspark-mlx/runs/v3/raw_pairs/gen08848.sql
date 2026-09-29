SELECT
    YEAR(order_date) AS order_year,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM orders
GROUP BY YEAR(order_date)
ORDER BY total_amount
