SELECT src.*, c.name, c.age, c.credit_limit FROM accounts src JOIN customers c ON src.customer_id = c.id WHERE c.name IS NOT NULL;
