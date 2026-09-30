SELECT p.city, COUNT(*) AS rows_total, SUM(src.quantity) AS total_quantity FROM inventory src JOIN stores p ON src.store_id = p.id GROUP BY ROLLUP (p.city) ORDER BY rows_total DESC NULLS LAST;
