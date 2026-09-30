SELECT
    o.order_date,
    o.amount,
    s.region,
    s.store_name,
    s.sqft,
    TRIM(o.status) AS clean_status
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
ORDER BY amount DESC
