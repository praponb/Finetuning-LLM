SELECT channel AS label
FROM orders
UNION ALL
SELECT city AS label
FROM suppliers
WHERE country <> 'DE'
