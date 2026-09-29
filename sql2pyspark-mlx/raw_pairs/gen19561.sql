WITH filtered AS (
    SELECT *, LEAST(stock_qty, cost) AS least_stock_qty_cost
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand = 'Initech'
),
enriched AS (
    SELECT
        *,
        LAG(stock_qty, 2) OVER (ORDER BY launch_date, id) AS prev_stock_qty,
        NTILE(5) OVER (ORDER BY price, id) AS price_bucket,
        NTILE(4) OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id) AS stock_qty_bucket,
        COUNT(stock_qty) OVER (PARTITION BY brand) AS brand_count_stock_qty
    FROM filtered
),
ranked AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        least_stock_qty_cost,
        prev_stock_qty,
        price_bucket,
        stock_qty_bucket,
        brand_count_stock_qty
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY supplier_id, brand ORDER BY cost DESC NULLS FIRST) = 1
),
deduped AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        supplier_id,
        least_stock_qty_cost,
        price_bucket,
        stock_qty_bucket
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty_bucket DESC, id) = 1
),
with_latest AS (
    SELECT s.*, x.order_item_unit_price
    FROM deduped s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    brand,
    supplier_id,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS stock_qty_per_acme
FROM with_latest
GROUP BY brand, supplier_id;
