WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        stock_qty - price AS stock_qty_minus_price,
        UPPER(category) AS category_upper
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT e.*, d.country
    FROM base e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(price, cost) AS greatest_price_cost
    FROM with_dims
    WHERE launch_date > DATE '2023-09-25'
      AND ARRAY_CONTAINS(tags, 'new')
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        category,
        cost,
        greatest_price_cost,
        price
    FROM filtered
) src
UNPIVOT (
    value FOR kpi IN (cost AS COST_VAL, greatest_price_cost, price)
);
