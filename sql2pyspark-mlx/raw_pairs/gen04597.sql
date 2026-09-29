SELECT s.*
FROM products s
WHERE EXISTS (
        SELECT 1
        FROM inventory x
        WHERE x.product_id = s.id
          AND x.last_restock >= s.launch_date
    );
