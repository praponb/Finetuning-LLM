WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.supplier_name,
        s.rating,
        s.country
    FROM products src
    LEFT JOIN suppliers s
        ON src.supplier_id <=> s.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (10, 24, 26)
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT s.*, d.city
    FROM not_flagged s
    JOIN suppliers d
        ON s.supplier_id <=> d.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (6, 18, 27, 35)
        )
)
SELECT
    s.id,
    s.category,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.rating,
    s.country,
    (SELECT MAX(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS max_product_stock_qty
FROM eligible s
ORDER BY id
