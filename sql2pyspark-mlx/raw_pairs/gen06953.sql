WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        UPPER(TRIM(product_name)) product_name_normalized,
        DATE_SUB(launch_date, 43) launch_date_minus_43d,
        COALESCE(brand, 'DIRECT') resolved_brand
    FROM products
    WHERE brand IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date >= s.launch_date_minus_43d
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY cost DESC NULLS LAST) AS row_num
        FROM not_flagged
    ) ranked
    WHERE row_num = 1
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.order_item_unit_price
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.brand,
        e.tags,
        e.supplier_id,
        e.product_name_normalized,
        e.launch_date_minus_43d,
        e.row_num,
        e.quantity,
        e.order_item_unit_price,
        d.country,
        d.city,
        d.supplier_name
    FROM with_last e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
)
SELECT
    s.id,
    s.brand,
    s.tags,
    s.supplier_id,
    s.product_name_normalized,
    s.launch_date_minus_43d,
    s.row_num,
    s.quantity,
    s.order_item_unit_price,
    (SELECT MIN(t2.cost) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS min_product_cost
FROM enriched s;
