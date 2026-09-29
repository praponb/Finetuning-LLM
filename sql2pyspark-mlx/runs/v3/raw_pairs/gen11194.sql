SELECT brand, supplier_id, COUNT(*) AS num_products
FROM products
WHERE category != 'Electronics' AND price > 1100 AND product_name LIKE 'Co%'
GROUP BY 1, 2
ORDER BY num_products DESC
