WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date <= '2023-11-01'
        )
),
ranked AS (
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST, id) = 1
),
with_latest AS (
    SELECT s.*, latest.product_price
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    supplier_name,
    country,
    rating
FROM with_latest
ORDER BY rating DESC NULLS FIRST, id
