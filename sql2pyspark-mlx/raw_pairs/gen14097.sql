WITH kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM products
    EXCEPT
    SELECT supplier_id
    FROM products
    WHERE SIZE(tags) > 1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    MAX(cost) FILTER (WHERE category = 'Fashion') AS fashion_max_cost,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count,
    SUM(stock_qty) FILTER (WHERE category = 'Books') AS books_total_stock_qty,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    BIT_AND(stock_qty) AS stock_qty_bit_and
FROM kept_rows
GROUP BY year_start
HAVING row_count < 1000;
