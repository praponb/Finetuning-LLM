SELECT category, supplier_id, COUNT(DISTINCT cost) AS unique_brands FROM products WHERE ARRAY_CONTAINS(tags, 'new') GROUP BY category, supplier_id HAVING COUNT(DISTINCT cost) > 5 ORDER BY category;
