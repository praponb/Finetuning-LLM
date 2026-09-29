SELECT supplier_id, COUNT(DISTINCT cost) AS unique_suppliers FROM products GROUP BY supplier_id HAVING AVG(price) < 900 ORDER BY unique_suppliers
