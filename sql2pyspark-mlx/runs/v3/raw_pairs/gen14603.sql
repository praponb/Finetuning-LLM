SELECT s.rating, p.price, p.product_name
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
WHERE s.country IN ('SG', 'TH', 'US') AND s.rating <= 2.7;
