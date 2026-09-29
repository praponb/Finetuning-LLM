SELECT id, rating, COALESCE(rating, 0) AS rating_filled FROM suppliers WHERE UPPER(supplier_name) LIKE '%IAM%' AND supplier_name LIKE '%Whol%';
