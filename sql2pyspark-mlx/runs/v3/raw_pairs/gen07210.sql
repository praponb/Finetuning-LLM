SELECT signup_date, 'customer' source
FROM customers
UNION ALL
SELECT last_restock signup_date, 'inventory' source
FROM inventory
WHERE quantity >= 175
ORDER BY signup_date DESC;
