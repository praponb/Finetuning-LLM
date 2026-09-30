WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        UPPER(category) AS category_upper,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE launch_date > TO_DATE('2023-05-27')
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_item_unit_price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.supplier_id,
        d.country,
        d.rating,
        d.city
    FROM with_last a
    LEFT JOIN suppliers d
        ON a.supplier_id <=> d.id
),
joined AS (
    -- Attach suppliers attributes
    SELECT s.*, d.rating AS supplier_rating
    FROM enriched s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, supplier_id ORDER BY supplier_rating DESC NULLS LAST, id) AS dedup_rank
        FROM joined
    ) ranked
    WHERE dedup_rank = 1
),
deduped_products AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_id,
        country,
        rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY rating DESC, id) AS dedup_rank_2
        FROM deduped
    ) x
    WHERE dedup_rank_2 = 1
)
SELECT
    s.id,
    s.supplier_id,
    s.country,
    CASE WHEN country RLIKE '^(SG|TH)$' THEN 'PRIORITY' ELSE 'CORE' END AS country_zone,
    (SELECT MIN(p2.stock_qty) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_stock_qty
FROM deduped_products s
ORDER BY min_product_stock_qty DESC;
