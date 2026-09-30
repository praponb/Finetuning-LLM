WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        ROUND(stock_qty, 0) AS stock_qty_rounded,
        COALESCE(price, 0) AS price_filled
    FROM products
    WHERE launch_date >= TO_DATE('2024-02-21')
      AND supplier_id IS NOT NULL
)
-- Grouped metrics
SELECT
    category,
    CORR(price, stock_qty) AS corr_price_stock_qty,
    SUM(CASE WHEN category = 'Sports' THEN stock_qty_rounded ELSE 0 END) AS sports_stock_qty_rounded
FROM base
GROUP BY category
HAVING corr_price_stock_qty < 0;
