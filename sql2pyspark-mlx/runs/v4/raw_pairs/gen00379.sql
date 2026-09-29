SELECT
    ss.id,
    ss.units,
    s.region,
    s.opened_date,
    s.store_name
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY opened_date, id
LIMIT 5;
