SELECT product_id, store_id, COUNT(units) units_count
FROM store_sales
WHERE product_id IN (3, 5, 9, 10) AND revenue > 2500
GROUP BY product_id, store_id;
