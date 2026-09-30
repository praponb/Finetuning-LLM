SELECT revenue
FROM store_sales
WHERE revenue IS NOT NULL
EXCEPT
SELECT cost
FROM shipments
WHERE cost IS NOT NULL;
