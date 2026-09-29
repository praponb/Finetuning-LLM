SELECT city, 'supplier' AS source FROM suppliers WHERE LENGTH(supplier_name) > 13 UNION SELECT city, 'store' AS source FROM stores
