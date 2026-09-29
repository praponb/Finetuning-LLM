SELECT last_restock
FROM inventory
WHERE quantity BETWEEN 144 AND 198
UNION ALL
SELECT signup_date
FROM customers;
