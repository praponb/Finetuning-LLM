WITH flattened AS (SELECT id, customer_id, created_at, INLINE(items) FROM carts) SELECT id, customer_id, created_at, price FROM flattened ORDER BY id
