SELECT opened_date
FROM stores
WHERE city <> 'Singapore'
UNION
SELECT sale_date
FROM store_sales
