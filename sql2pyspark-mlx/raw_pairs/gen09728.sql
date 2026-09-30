WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        ADD_MONTHS(sale_date, 3) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date > DATE '2023-04-23'
      AND revenue BETWEEN 1100 AND 2500
),
with_dims AS (
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        b.category
    FROM cleaned a
    JOIN products b
        ON a.product_id = b.id
    WHERE SIZE(b.tags) >= 1
),
valid AS (
    SELECT
        *,
        UPPER(category) AS category_upper,
        COALESCE(category, 'OTHER') AS category_filled
    FROM with_dims
    WHERE store_id IN (1, 5)
),
valid_store_sales AS (
    SELECT
        *,
        INITCAP(TRIM(category)) AS category_title,
        CASE WHEN category = 'Books' THEN 1 ELSE 0 END AS is_books
    FROM valid
    WHERE category IS DISTINCT FROM 'Sports'
)
SELECT
    CASE WHEN is_books >= 83 THEN 'LARGE' ELSE 'SMALL' END AS is_books_tier,
    product_id,
    COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(is_books, 0.9) AS is_books_p90,
    GROUPING(CASE WHEN is_books >= 83 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM valid_store_sales
GROUP BY GROUPING SETS ((CASE WHEN is_books >= 83 THEN 'LARGE' ELSE 'SMALL' END, product_id), (CASE WHEN is_books >= 83 THEN 'LARGE' ELSE 'SMALL' END), ())
