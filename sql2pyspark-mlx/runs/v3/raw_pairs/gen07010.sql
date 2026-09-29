SELECT s.rating, p.price FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id ORDER BY 1 DESC;
