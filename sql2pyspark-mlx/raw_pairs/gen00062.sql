SELECT dim.city, COUNT(*) AS rows_total, SUM(e.quantity) AS total_quantity, GROUPING_ID() AS level_id FROM inventory e LEFT JOIN stores dim ON e.store_id = dim.id GROUP BY ROLLUP (dim.city);
