SELECT ss.units, p.category FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE p.brand = 'Umbrella' OR p.product_name LIKE '%Offic%'
