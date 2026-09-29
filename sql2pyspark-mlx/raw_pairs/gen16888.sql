SELECT status, COUNT(DISTINCT amount) unique_stores, COUNT(discount) discount_count FROM orders WHERE order_date > '2023-07-29' AND shipping_fee > 29 GROUP BY status
