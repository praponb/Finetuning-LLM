SELECT units, store_id
FROM store_sales
WHERE MONTH(sale_date) IN (1, 9, 12)
EXCEPT
SELECT units, store_id
FROM store_sales
WHERE units >= 23
ORDER BY store_id;
