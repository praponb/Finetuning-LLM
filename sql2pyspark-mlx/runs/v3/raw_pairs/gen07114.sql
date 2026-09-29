SELECT SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) AS acme_cost FROM products ORDER BY acme_cost;
