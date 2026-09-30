WITH base AS (
    SELECT *, TRIM(brand) clean_brand
    FROM products
    WHERE launch_date >= DATE '2023-12-28'
      AND supplier_id IS NULL
)
SELECT
    s.*,
    latest.last_restock,
    latest.inventory_quantity
FROM base s
JOIN LATERAL (
        SELECT c.last_restock, c.quantity AS inventory_quantity
        FROM inventory c
        WHERE c.product_id = s.id
        ORDER BY c.quantity DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
