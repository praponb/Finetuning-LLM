SELECT s.*
FROM suppliers s
WHERE NOT EXISTS (
        SELECT 1
        FROM products x
        WHERE x.supplier_id = s.id
          AND SIZE(x.tags) >= 1
    )
