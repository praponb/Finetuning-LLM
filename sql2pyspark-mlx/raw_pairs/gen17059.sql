WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        REGEXP_REPLACE(product_name, ' ', '') AS product_name_slug,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM products
    WHERE launch_date >= DATE '2023-06-06'
      AND supplier_id IS NOT NULL
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.price,
        s.stock_qty,
        s.tags,
        s.product_name_slug,
        s.resolved_brand,
        x.quantity,
        x.unit_price
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        brand,
        price,
        stock_qty,
        quantity,
        unit_price * quantity AS line_total,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM with_latest
    WHERE brand = 'Stark'
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.stock_qty,
        s.brand_filled,
        x.units,
        x.store_sale_sale_date
    FROM base s
    JOIN LATERAL (
            SELECT c.units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.revenue < 2000
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        brand,
        stock_qty,
        units,
        store_sale_sale_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY units ASC) AS latest_rank
        FROM not_flagged
    ) t
    WHERE latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN units >= 38 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    ROUND(AVG(units), 2) AS avg_units,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev,
    COUNT(DISTINCT id) AS distinct_ids
FROM ranked
GROUP BY units_tier
HAVING STDDEV_SAMP(stock_qty) >= 0
ORDER BY avg_units ASC;
