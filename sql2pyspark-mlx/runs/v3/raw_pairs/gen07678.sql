SELECT
    YEAR(o.order_date) AS order_year,
    o.status,
    MAX(oi.quantity) AS max_quantity,
    MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.quantity = 8
GROUP BY YEAR(o.order_date), o.status
ORDER BY 1, 3 DESC;
