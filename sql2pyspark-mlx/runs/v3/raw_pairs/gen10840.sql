SELECT id, stock_qty, category, CASE WHEN brand IN ('Initech', 'Globex') THEN 'Priority' ELSE 'Other' END brand_group FROM products WHERE stock_qty <= 366 ORDER BY brand_group DESC;
