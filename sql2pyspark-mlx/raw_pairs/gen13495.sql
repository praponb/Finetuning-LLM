WITH valid AS (
    SELECT
        *,
        ARRAY_JOIN(tags, '|') tags_text,
        CEIL(stock_qty) stock_qty_whole
    FROM products
    WHERE launch_date >= DATE '2023-08-26'
      AND brand IS NOT NULL
),
enriched AS (
    SELECT
        e.id,
        e.category,
        e.brand,
        e.price,
        e.cost,
        e.stock_qty,
        e.tags,
        e.supplier_id,
        e.tags_text,
        d.rating,
        d.supplier_name,
        d.city
    FROM valid e
    LEFT JOIN suppliers d
        ON e.supplier_id <=> d.id
),
enriched_products AS (
    SELECT
        *,
        AVG(stock_qty) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_stock_qty,
        COUNT(stock_qty) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_stock_qty,
        NTILE(4) OVER (ORDER BY rating DESC, id) rating_bucket
    FROM enriched
),
matched_keys AS (
    SELECT brand, id
    FROM enriched_products
    EXCEPT DISTINCT
    SELECT brand, id
    FROM products
    WHERE SIZE(tags) > 1
),
verified AS (
    SELECT s.*
    FROM enriched_products s
    JOIN matched_keys k ON s.brand = k.brand AND s.id = k.id
),
filtered AS (
    SELECT
        *,
        LENGTH(category) category_length,
        COALESCE(city, 'OTHER') city_clean
    FROM verified
    WHERE brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Umbrella'
)
SELECT
    s.country,
    src.category,
    COUNT(*) AS num_rows,
    SUM(src.price) AS total_price,
    MAX(src.cost) AS max_cost
FROM filtered src
LEFT JOIN suppliers s ON src.supplier_id = s.id
GROUP BY s.country, src.category;
