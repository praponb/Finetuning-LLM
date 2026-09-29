WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        INITCAP(TRIM(brand)) AS brand_title
    FROM products
    WHERE stock_qty <= 318
      AND launch_date > TO_DATE('2023-03-20')
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id = 23
        )
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.category,
        s.price,
        s.cost,
        s.stock_qty,
        s.tags,
        s.supplier_id,
        s.brand_title,
        s.quantity,
        p2.city
    FROM not_flagged s
    LEFT JOIN suppliers p2
        ON s.supplier_id = p2.id
    WHERE p2.city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id IN (2, 5, 6, 7)
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.rating,
        s.city AS supplier_city
    FROM eligible a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
)
SELECT
    id,
    cost,
    tags,
    brand_title,
    quantity,
    city,
    rating,
    supplier_city
FROM enriched
ORDER BY city DESC NULLS LAST, supplier_city;
