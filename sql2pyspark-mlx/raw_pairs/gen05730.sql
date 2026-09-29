SELECT
    s.id,
    s.city,
    s.store_name,
    s.opened_date
FROM stores s
WHERE s.region NOT IN ('North', 'East') AND s.id IN (SELECT store_id FROM store_sales);
