WITH enriched AS (
    -- Attach suppliers attributes
    SELECT a.*, s.supplier_name
    FROM products a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.quantity,
        x.inventory_last_restock
    FROM enriched s
    JOIN LATERAL (
            SELECT c.quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.rating,
        s.country,
        s.city
    FROM with_last a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p.city AS supplier_city,
        p.rating AS supplier_rating,
        p.supplier_name AS supplier_supplier_name
    FROM enriched_products s
    LEFT JOIN suppliers p
        ON s.supplier_id = p.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        category,
        COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
        BIT_AND(stock_qty) AS stock_qty_bit_and,
        MAX(rating) AS max_rating
    FROM enriched_products_2
    GROUP BY category
)
SELECT
    category,
    stark_count,
    max_rating,
    CASE WHEN category RLIKE '^(Fashion|Books)$' THEN 'EXTENDED' WHEN category RLIKE '^(Electronics|Home)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS category_zone
FROM daily
ORDER BY stark_count DESC NULLS LAST, max_rating
