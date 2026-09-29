SELECT
    ss.id,
    ss.product_id,
    ss.units,
    ss.sale_date
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE sqft BETWEEN 6767 AND 13393);
