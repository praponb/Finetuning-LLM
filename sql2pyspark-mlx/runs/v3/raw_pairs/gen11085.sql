SELECT status, MAX(discount) AS max_discount, MAX(order_date) AS latest_order_date FROM orders WHERE status IN ('SHIPPED', 'RETURNED') AND store_id IN (8, 18, 38) GROUP BY 1;
