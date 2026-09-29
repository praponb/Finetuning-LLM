SELECT src.*, c.segment
FROM accounts src
LEFT JOIN customers c
    ON src.customer_id = c.id
