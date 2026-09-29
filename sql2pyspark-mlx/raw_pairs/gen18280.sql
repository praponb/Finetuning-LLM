WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        SORT_ARRAY(tags) AS sorted_tags,
        TRIM(brand) AS clean_brand
    FROM products
    WHERE brand IS NOT DISTINCT FROM 'Initech'
      AND launch_date >= '2024-01-01'
),
enriched AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        tags,
        sorted_tags,
        LAG(price, 2, 0) OVER w_part AS prev_price,
        AVG(price) OVER w_recent AS price_avg_5,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist
    FROM valid
    WHERE brand = 'Initech'
    WINDOW w_part AS (PARTITION BY MONTH(launch_date), supplier_id ORDER BY launch_date ASC, id ASC), w_recent AS (PARTITION BY category, supplier_id ORDER BY cost ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
with_latest AS (
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.tags,
        s.sorted_tags,
        s.price_avg_5,
        s.cost_cume_dist,
        top1.order_item_unit_price
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id IN (5, 6, 7, 9)
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.quantity >= 7
        )
),
with_last AS (
    SELECT s.*, top1.inventory_quantity
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched_products AS (
    SELECT
        id,
        sorted_tags,
        price_avg_5,
        CUME_DIST() OVER (PARTITION BY brand ORDER BY order_item_unit_price ASC NULLS FIRST) AS order_item_unit_price_cume_dist,
        ROW_NUMBER() OVER (ORDER BY cost_cume_dist DESC, id ASC) AS seq_no
    FROM with_last
)
SELECT
    id,
    sorted_tags,
    order_item_unit_price_cume_dist,
    seq_no
FROM enriched_products
ORDER BY seq_no DESC;
