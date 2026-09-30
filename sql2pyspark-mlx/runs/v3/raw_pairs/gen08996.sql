SELECT p.supplier_id, t.units_range
FROM products p
LEFT OUTER JOIN (SELECT product_id, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.units_range > 75;
