SELECT o.amount, c.name, s.opened_date, s.sqft FROM orders o LEFT OUTER JOIN customers c ON o.customer_id = c.id LEFT OUTER JOIN stores s ON o.store_id = s.id
