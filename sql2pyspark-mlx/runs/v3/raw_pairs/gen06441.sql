SELECT delivered_date FROM shipments WHERE shipped_date BETWEEN '2023-06-15' AND '2023-11-11' INTERSECT SELECT order_date FROM orders ORDER BY delivered_date;
