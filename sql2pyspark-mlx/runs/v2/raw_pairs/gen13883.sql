SELECT c.credit_limit, c.is_active, o.shipping_fee FROM customers c INNER JOIN orders o ON c.id = o.customer_id WHERE o.channel IN ('web', 'mobile');
