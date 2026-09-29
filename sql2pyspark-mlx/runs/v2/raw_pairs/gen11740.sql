SELECT p.id, p.cost, p.price, p.category, s.rating, s.supplier_name, s.country FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE s.country NOT IN ('UK', 'US')
