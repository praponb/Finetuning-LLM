SELECT ss.id, ss.revenue, ss.units, p.price FROM store_sales ss LEFT JOIN products p ON p.id = ss.product_id WHERE ARRAY_CONTAINS(p.tags, 'clearance');
