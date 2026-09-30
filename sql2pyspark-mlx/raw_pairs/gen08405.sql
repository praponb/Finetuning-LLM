WITH base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        DATE_TRUNC('week', launch_date) AS launch_date_period
    FROM products
    WHERE price < 830
)
SELECT
    id,
    category,
    brand,
    price,
    stock_qty,
    tags
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost, id) AS rank_in_group
    FROM base
) x
WHERE rank_in_group = 1
