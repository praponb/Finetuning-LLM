SELECT status
FROM payments
WHERE TO_DATE(paid_at) >= '2024-03-01'
EXCEPT
SELECT product_name
FROM products;
