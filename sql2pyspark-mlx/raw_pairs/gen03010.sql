SELECT
    s.id,
    s.region,
    s.store_name,
    s.sqft
FROM stores s
WHERE MONTH(s.opened_date) IN (5, 10, 12) AND s.id IN (SELECT store_id FROM store_sales WHERE store_id = 3)
ORDER BY 3 ASC, region ASC, id ASC
LIMIT 5
