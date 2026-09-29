WITH latest_match AS (
    SELECT
        s.*,
        latest.store_sale_sale_date,
        latest.store_sale_units
    FROM products s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    SELECT
        *,
        CASE WHEN price >= 1140 THEN 'High' WHEN price >= 270 THEN 'Medium' ELSE 'Low' END AS price_band,
        ARRAY_CONTAINS(tags, 'clearance') AS is_clearance
    FROM latest_match
    WHERE brand IS NOT DISTINCT FROM 'Acme'
),
with_latest AS (
    SELECT
        s.*,
        top1.store_sale_sale_date_2,
        top1.store_sale_units_2
    FROM base s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date_2, c.units AS store_sale_units_2
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_products AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.cost,
        s.launch_date,
        s.supplier_id,
        s.store_sale_units,
        s.is_clearance,
        latest.order_item_quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    SELECT
        id,
        product_name,
        category,
        cost,
        store_sale_units,
        order_item_quantity,
        FIRST_VALUE(store_sale_units) IGNORE NULLS OVER (PARTITION BY MONTH(launch_date) ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_store_sale_units,
        MIN(store_sale_units) OVER (PARTITION BY YEAR(launch_date), supplier_id) AS launch_date_year_min_store_sale_units,
        SUM(store_sale_units) OVER (PARTITION BY category ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_store_sale_units,
        SUM(cost) OVER (PARTITION BY supplier_id, category ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        NTILE(5) OVER (ORDER BY order_item_quantity ASC, id ASC) AS order_item_quantity_bucket
    FROM with_latest_products
    WHERE store_sale_units = 39
),
with_latest_products_2 AS (
    SELECT
        s.*,
        x.units,
        x.store_sale_sale_date
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
report AS (
    SELECT
        id,
        category,
        cost,
        store_sale_units,
        order_item_quantity,
        first_known_store_sale_units,
        launch_date_year_min_store_sale_units,
        running_store_sale_units,
        cumulative_cost
    FROM with_latest_products_2
    ORDER BY order_item_quantity ASC NULLS LAST
)
SELECT
    category,
    order_item_quantity,
    first_known_store_sale_units,
    launch_date_year_min_store_sale_units,
    running_store_sale_units,
    cumulative_cost,
    CASE WHEN category RLIKE '^(Home|Sports)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS category_zone
FROM report;
