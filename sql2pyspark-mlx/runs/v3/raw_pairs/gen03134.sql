SELECT sale_date
FROM store_sales
WHERE units != 29
UNION ALL
SELECT delivered_date sale_date
FROM shipments
WHERE order_id IN (4, 35)
