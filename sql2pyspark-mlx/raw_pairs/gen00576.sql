WITH kept AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IS NULL
        )
),
with_latest AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.launch_date,
        x.quantity
    FROM kept s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    SELECT
        *,
        CONCAT(category, ' ', brand) category_brand,
        COALESCE(brand, 'DIRECT') brand_clean
    FROM with_latest
    WHERE launch_date >= TO_DATE('2023-06-25')
      AND brand IS DISTINCT FROM 'Globex'
),
eligible AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (9, 27, 35)
        )
),
stats AS (
    SELECT
        CASE WHEN stock_qty >= 444 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
        BIT_OR(stock_qty) AS stock_qty_bit_or,
        CORR(price, stock_qty) AS corr_price_stock_qty,
        COUNT_IF(brand = 'Stark') AS stark_events
    FROM eligible
    GROUP BY stock_qty_tier
)
SELECT
    stock_qty_tier,
    stock_qty_bit_or,
    corr_price_stock_qty,
    stark_events
FROM stats
ORDER BY corr_price_stock_qty DESC
