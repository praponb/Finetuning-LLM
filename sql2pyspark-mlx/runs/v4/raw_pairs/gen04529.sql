SELECT src.*, c.segment, c.email FROM accounts src LEFT JOIN customers c ON src.customer_id = c.id;
