SELECT supplier_id, MAX(cost) AS max_cost, MAX(price) AS max_price FROM products GROUP BY supplier_id
