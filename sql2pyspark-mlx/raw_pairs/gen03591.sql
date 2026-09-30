WITH filtered AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        COALESCE(cost, 0) cost_filled,
        cost - price margin
    FROM products
    WHERE launch_date > TO_DATE('2023-08-03')
      AND category = 'Electronics'
)
SELECT
    id,
    category,
    brand,
    price,
    cost,
    launch_date,
    cost_filled,
    margin,
    LEAD(price, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date) ORDER BY price, id) next_price,
    AVG(margin) OVER w_part margin_avg_5_prior
FROM filtered
WHERE category != 'Grocery'
WINDOW w_part AS (ORDER BY stock_qty, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
