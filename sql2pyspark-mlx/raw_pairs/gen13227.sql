WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(product_name, ' - ', category) AS product_name_category,
        GREATEST(price, stock_qty) AS greatest_price_stock_qty
    FROM products
    WHERE launch_date >= TO_DATE('2023-09-25')
      AND UPPER(product_name) LIKE '%UNN%'
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.rating,
        s.city
    FROM base a
    LEFT JOIN suppliers s
        ON a.supplier_id <=> s.id
    WHERE s.rating < 3.6
),
joined_products AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.country,
        b.rating AS supplier_rating,
        b.city AS supplier_city
    FROM joined e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        launch_date,
        rating,
        city,
        country,
        supplier_rating,
        supplier_city,
        MAX(price) OVER (PARTITION BY country) AS country_max_price,
        LAST_VALUE(supplier_city) IGNORE NULLS OVER (ORDER BY greatest_price_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_supplier_city
    FROM joined_products
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, category ORDER BY supplier_rating DESC, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    product_name,
    category,
    rating,
    supplier_rating,
    country_max_price
FROM latest
ORDER BY category DESC
