SELECT id, category, COUNT(*) OVER (PARTITION BY category) AS category_count
FROM products
ORDER BY category_count DESC, id DESC
