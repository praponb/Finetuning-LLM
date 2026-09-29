SELECT id, supplier_name, CAST(rating AS INT) AS rating_int FROM suppliers WHERE city IN ('Berlin', 'Chiang Mai')
