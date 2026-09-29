SELECT COUNT(rating) AS rating_count, COUNT(*) AS num_suppliers
FROM suppliers
WHERE country IN ('SG', 'JP') AND LOWER(supplier_name) LIKE '%iam%'
