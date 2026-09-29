SELECT ss.id, ss.store_id, ss.units, p.cost FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE MONTH(p.launch_date) IN (4, 10, 11) AND MONTH(ss.sale_date) IN (1, 3, 9)
