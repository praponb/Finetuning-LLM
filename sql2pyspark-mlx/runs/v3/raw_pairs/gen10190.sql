SELECT products.brand, products.supplier_id, SUM(store_sales.units) AS total_units
FROM store_sales
LEFT JOIN stores ON store_sales.store_id = stores.id
LEFT JOIN products ON store_sales.product_id = products.id
WHERE LENGTH(stores.store_name) > 14
GROUP BY products.brand, products.supplier_id
HAVING SUM(store_sales.units) >= 220
ORDER BY total_units DESC, products.brand DESC;
