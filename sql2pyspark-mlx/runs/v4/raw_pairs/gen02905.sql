WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(launch_date, 'MMM yyyy') AS launch_date_label,
        CONCAT_WS(',', product_name, category) AS product_name_category_key,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM products
    WHERE brand IS NOT NULL
      AND launch_date >= '2024-10-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cost, 1) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_cost,
        NTILE(3) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        SUM(stock_qty) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        LEAD(price, 1) OVER (PARTITION BY brand ORDER BY stock_qty, id) AS next_price
    FROM filtered
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY category ORDER BY cumulative_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        SUM(cost_bucket) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_bucket,
        LAG(next_price, 1) OVER (PARTITION BY category ORDER BY next_price, id) AS prev_next_price,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS price_bucket
    FROM enriched
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, category ORDER BY next_price DESC, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p.rating,
        p.country
    FROM ranked s
    LEFT JOIN suppliers p
        ON s.supplier_id = p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        category,
        brand,
        country,
        rating
    FROM joined
) src
PIVOT (
    MAX(rating) AS amount,
    COUNT(1) AS item_count
    FOR country IN ('DE', 'TH')
);
