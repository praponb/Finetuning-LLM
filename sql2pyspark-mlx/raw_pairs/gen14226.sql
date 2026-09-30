SELECT channel, unique_stores
FROM (
    SELECT channel, COUNT(DISTINCT shipping_fee) AS unique_stores
    FROM orders
    WHERE YEAR(order_date) = 2025
    GROUP BY channel
) AS t
WHERE unique_stores >= 5
