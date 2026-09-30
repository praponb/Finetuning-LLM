SELECT s.city, s.supplier_name, p.category, p.price, p.supplier_id, SORT_ARRAY(p.tags) AS sorted_tags FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id ORDER BY p.category ASC
