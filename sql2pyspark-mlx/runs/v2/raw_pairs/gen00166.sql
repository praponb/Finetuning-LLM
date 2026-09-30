SELECT s.country, p.product_name, p.launch_date, p.cost FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id WHERE s.rating >= 3.8 AND p.cost BETWEEN 250 AND 570
