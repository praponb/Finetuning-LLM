SELECT products.category, suppliers.city, suppliers.rating
FROM products
INNER JOIN suppliers ON suppliers.id = products.supplier_id
ORDER BY suppliers.rating DESC;
