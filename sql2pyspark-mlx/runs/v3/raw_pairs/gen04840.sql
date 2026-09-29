SELECT
    o.id,
    o.status,
    o.amount,
    o.shipping_fee,
    s.city,
    s.region,
    DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_date_label
FROM orders o
RIGHT JOIN stores s ON s.id = o.store_id
WHERE o.amount BETWEEN 450 AND 880 AND s.region = 'East'
ORDER BY region;
