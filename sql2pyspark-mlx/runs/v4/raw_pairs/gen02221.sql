WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        price * cost AS margin,
        LEAST(stock_qty, price) AS least_stock_qty_price
    FROM products
    WHERE brand IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        margin,
        LENGTH(category) AS category_length
    FROM filtered
    WHERE launch_date >= DATE '2023-09-02'
      AND launch_date >= '2024-09-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        margin,
        category_length,
        ROW_NUMBER() OVER (ORDER BY launch_date, id) AS row_seq,
        LEAD(stock_qty, 1) OVER w_running AS next_stock_qty
    FROM valid
    WINDOW w_running AS (ORDER BY category_length, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY category_length NULLS LAST, id) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    COUNT(*) AS cnt,
    SUM(CASE WHEN category = 'Books' THEN price ELSE 0 END) AS books_price,
    BIT_OR(stock_qty) AS stock_qty_bit_or
FROM deduped
GROUP BY DATE_TRUNC('QUARTER', launch_date)
HAVING SUM(CASE WHEN category = 'Books' THEN price ELSE 0 END) >= 0
