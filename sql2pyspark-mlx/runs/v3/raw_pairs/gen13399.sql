SELECT order_id FROM shipments WHERE delivered_date BETWEEN '2024-07-01' AND '2024-09-13' EXCEPT SELECT order_id FROM shipments WHERE shipped_date >= '2023-07-18' ORDER BY order_id DESC;
