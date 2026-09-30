SELECT id, launch_date, RANK() OVER (ORDER BY launch_date DESC) rnk
FROM products
WHERE ARRAY_CONTAINS(tags, 'bestseller');
