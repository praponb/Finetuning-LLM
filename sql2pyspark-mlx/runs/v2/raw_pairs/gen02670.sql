SELECT s.country, COUNT(DISTINCT p.cost) AS unique_brands FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE SIZE(p.tags) >= 2 GROUP BY s.country;
