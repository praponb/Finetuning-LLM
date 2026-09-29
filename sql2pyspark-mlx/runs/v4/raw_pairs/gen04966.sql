SELECT suppliers.city, COUNT(*) AS cnt FROM products LEFT OUTER JOIN suppliers ON suppliers.id = products.supplier_id GROUP BY suppliers.city ORDER BY suppliers.city
