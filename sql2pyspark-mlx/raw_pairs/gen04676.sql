WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ARRAY_CONTAINS(tags, 'eco') AS is_eco,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE category = 'Fashion'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY cost DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.unit_price
    FROM deduped s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    -- Attach suppliers attributes
    SELECT s.*, p2.rating
    FROM with_latest s
    LEFT JOIN suppliers p2
        ON s.supplier_id = p2.id
    WHERE p2.city = 'London'
)
SELECT
    s.id,
    s.product_name,
    s.brand,
    s.cost,
    s.launch_date,
    s.supplier_id,
    s.rank_in_group,
    s.unit_price,
    s.rating,
    (SELECT MIN(p2.stock_qty) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_stock_qty
FROM joined s
ORDER BY cost DESC
