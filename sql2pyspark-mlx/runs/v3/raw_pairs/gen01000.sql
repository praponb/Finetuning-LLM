SELECT
    s.city,
    s.sqft,
    s.opened_date,
    i.last_restock,
    i.store_id
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
WHERE MONTH(s.opened_date) IN (7, 9, 10) OR UPPER(s.store_name) LIKE '%LD %'
