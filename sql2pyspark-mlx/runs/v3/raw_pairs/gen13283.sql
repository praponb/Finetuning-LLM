SELECT category, COUNT(DISTINCT price) AS unique_suppliers FROM products GROUP BY category;
