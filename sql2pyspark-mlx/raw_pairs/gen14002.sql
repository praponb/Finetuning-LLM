WITH valid AS (
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
        ARRAY_CONTAINS(tags, 'premium') AS is_premium,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE launch_date >= TO_DATE('2023-04-19')
),
with_windows AS (
    SELECT
        id,
        product_name,
        brand,
        stock_qty,
        tags,
        supplier_id,
        LAG(price) OVER (PARTITION BY brand ORDER BY stock_qty, id) AS prev_price,
        LEAD(stock_qty) OVER (PARTITION BY brand ORDER BY stock_qty, id) AS next_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty, id) AS event_seq
    FROM valid
),
deduped AS (
    SELECT
        id,
        product_name,
        brand,
        stock_qty,
        prev_price,
        next_stock_qty
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY prev_price DESC, id) = 1
),
deduped_products AS (
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) = 1
)
SELECT
    id,
    brand,
    prev_price
FROM deduped_products
ORDER BY id DESC NULLS FIRST
