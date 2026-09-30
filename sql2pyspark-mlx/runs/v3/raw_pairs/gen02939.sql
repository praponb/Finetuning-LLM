SELECT i.id, i.quantity, p.price, p.product_name, p.cost * p.price margin FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE p.brand NOT IN ('Umbrella', 'Initech')
