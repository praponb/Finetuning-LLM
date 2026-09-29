WITH base AS (
    SELECT *, stock_qty * price AS stock_qty_times_price
    FROM products
    WHERE brand IS DISTINCT FROM 'Initech'
      AND price < 1190
)
SELECT *
FROM (
    SELECT
        id,
        category,
        cost,
        stock_qty_times_price,
        price
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    value FOR kpi IN (cost, stock_qty_times_price, price AS PRICE_VAL)
);
