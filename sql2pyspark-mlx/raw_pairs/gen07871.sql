WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CONCAT(product_name, ' - ', brand) AS product_name_brand,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE brand IS DISTINCT FROM 'Initech'
      AND launch_date > DATE '2023-09-01'
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.inventory_last_restock
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.unit_price
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(stock_qty) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_6_prior,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share,
        LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        LEAD(cost, 1) OVER (ORDER BY price, id) AS next_cost
    FROM with_latest
    WHERE stock_qty >= 153
)
SELECT
    id,
    price,
    cost,
    tags,
    supplier_id,
    product_name_brand,
    inventory_last_restock,
    unit_price,
    stock_qty_avg_6_prior
FROM scored;
