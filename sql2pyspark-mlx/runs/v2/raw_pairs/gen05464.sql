SELECT ss.id, ss.units
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date <= '2023-08-26');
