WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        COALESCE(cost, 0) cost_filled
    FROM products
    WHERE brand IS NOT NULL
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_item_unit_price
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT brand, id
    FROM latest_match
    EXCEPT
    SELECT brand, id
    FROM products
    WHERE LENGTH(product_name) >= 13
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN remaining_keys k ON s.brand = k.brand AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        cost_filled,
        order_item_unit_price,
        ROUND(price / SUM(price) OVER (PARTITION BY category), 4) price_share,
        MAX(order_item_unit_price) OVER w_ordered brand_max_order_item_unit_price
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY brand, category)
)
SELECT
    id,
    product_name,
    category,
    price,
    launch_date,
    cost_filled,
    order_item_unit_price,
    brand_max_order_item_unit_price
FROM enriched
ORDER BY brand_max_order_item_unit_price DESC;
