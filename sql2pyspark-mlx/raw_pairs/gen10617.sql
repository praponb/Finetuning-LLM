SELECT d.brand, d.category, COUNT(*) AS cnt, MIN(a.quantity) AS min_quantity FROM inventory a INNER JOIN products d ON a.product_id = d.id GROUP BY d.brand, d.category ORDER BY cnt NULLS LAST;
