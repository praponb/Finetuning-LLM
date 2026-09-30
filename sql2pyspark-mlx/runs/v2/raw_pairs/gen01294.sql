SELECT status, COUNT(DISTINCT shipping_fee) AS unique_customers, AVG(amount) AS avg_amount FROM orders GROUP BY status;
