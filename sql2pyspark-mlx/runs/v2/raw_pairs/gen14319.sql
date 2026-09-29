SELECT
    s.id,
    s.sqft,
    s.region,
    s.city
FROM stores s
WHERE YEAR(s.opened_date) >= 2023 AND s.id IN (SELECT store_id FROM inventory WHERE product_id IN (4, 5, 7));
