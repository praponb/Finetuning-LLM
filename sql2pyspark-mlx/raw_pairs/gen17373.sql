WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        TRIM(supplier_name) AS clean_supplier_name
    FROM suppliers
    WHERE country IN ('UK', 'SG')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        NTILE(10) OVER (PARTITION BY city ORDER BY rating DESC, id) AS rating_bucket
    FROM base
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.rating_avg_2,
        x.product_category
    FROM enriched s
    JOIN LATERAL (
            SELECT c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.price,
        latest.product_cost
    FROM with_last s
    JOIN LATERAL (
            SELECT c.price, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    product_category,
    SIZE(COLLECT_SET(product_category)) AS num_product_categories,
    SORT_ARRAY(COLLECT_SET(product_category)) AS product_category_list,
    COUNT(*) AS num_rows
FROM with_last_suppliers
GROUP BY product_category
ORDER BY num_rows DESC;
