SELECT
    YEAR(o.order_date) AS order_year,
    DATE_TRUNC('MONTH', o.order_date) AS order_month,
    MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range,
    MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.quantity = 8
GROUP BY YEAR(o.order_date), DATE_TRUNC('MONTH', o.order_date)
ORDER BY 1, 3 DESC;
