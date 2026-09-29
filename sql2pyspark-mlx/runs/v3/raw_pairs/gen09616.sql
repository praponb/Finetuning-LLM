SELECT s.country, p.cost, p.brand, SIZE(p.tags) AS tags_count FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id;
