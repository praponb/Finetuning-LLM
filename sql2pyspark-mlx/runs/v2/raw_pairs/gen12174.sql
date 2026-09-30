SELECT suppliers.id, suppliers.rating FROM suppliers WHERE suppliers.id IN (SELECT supplier_id FROM products)
