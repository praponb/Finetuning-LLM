WITH with_last AS (
    SELECT s.*, x.unit_price
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    COUNT(DISTINCT supplier_id) AS distinct_supplier_ids,
    CORR(stock_qty, cost) AS corr_stock_qty_cost
FROM with_last
GROUP BY DATE_TRUNC('YEAR', launch_date)
HAVING CORR(stock_qty, cost) < 0
ORDER BY stock_qty_bit_and DESC;
