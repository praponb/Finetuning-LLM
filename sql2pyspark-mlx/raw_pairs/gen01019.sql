SELECT id
FROM store_sales
EXCEPT ALL
SELECT id
FROM store_sales
WHERE units = 35
