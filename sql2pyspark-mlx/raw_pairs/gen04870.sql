WITH base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        supplier_id,
        ROUND(cost * 100, 2) AS cost_scaled,
        TRIM(brand) AS clean_brand,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-03-28'
      AND cost <= 420
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) = 1
),
remaining_keys AS (
    SELECT category, supplier_id
    FROM ranked
    EXCEPT ALL
    SELECT category, supplier_id
    FROM products
    WHERE LENGTH(product_name) > 11
),
kept_rows AS (
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.category = k.category AND s.supplier_id = k.supplier_id
),
filtered AS (
    SELECT
        id,
        category,
        brand,
        price,
        supplier_id,
        brand_filled,
        LOWER(category) AS category_lower
    FROM kept_rows
    WHERE launch_date > TO_DATE('2023-09-05')
      AND supplier_id IS NOT NULL
),
matched_keys AS (
    SELECT brand, supplier_id
    FROM filtered
    EXCEPT
    SELECT brand, supplier_id
    FROM products
    WHERE price IS NULL
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
)
SELECT *
FROM (
    SELECT
        brand,
        category,
        price
    FROM reconciled
) src
PIVOT (
    MAX(price) AS amount
    FOR category IN ('Grocery', 'Home', 'Books')
);
