SELECT p.id, p.method, p.status, o.store_id, CAST(p.amount AS INT) AS amount_int FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE p.method = 'promptpay';
