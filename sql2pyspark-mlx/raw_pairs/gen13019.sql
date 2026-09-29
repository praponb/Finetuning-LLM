WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        DATE_TRUNC('week', sale_date) AS sale_date_period
    FROM store_sales
    WHERE sale_date > DATE '2023-08-23'
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.sale_date_period,
        d.category,
        d.brand
    FROM cleaned src
    JOIN products d
        ON src.product_id = d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY sale_date_period, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.sale_date,
        s.sale_date_period,
        s.brand,
        s.latest_rank,
        d.category AS product_category
    FROM latest s
    JOIN products d
        ON s.product_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_dims
    EXCEPT DISTINCT
    SELECT id
    FROM store_sales
    WHERE revenue <= 3000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', sale_date_period) AS week_start,
    COUNT(*) FILTER (WHERE product_category = 'Home') AS home_rows,
    COUNT(*) FILTER (WHERE product_category = 'Books') AS books_rows,
    (UNIX_TIMESTAMP(MAX(sale_date_period)) - UNIX_TIMESTAMP(MIN(sale_date_period))) / 60 AS duration_min
FROM kept_rows
GROUP BY week_start
ORDER BY books_rows;
