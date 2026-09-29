WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue * units revenue_per_unit
    FROM store_sales
    WHERE store_id IN (5, 6)
),
joined AS (
    SELECT s.*, b.sqft
    FROM cleaned s
    LEFT JOIN stores b
        ON s.store_id = b.id
    WHERE b.city <> 'Chiang Mai'
),
with_dims AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        src.revenue,
        src.revenue_per_unit,
        s.city,
        s.store_name
    FROM joined src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
joined_store_sales AS (
    SELECT e.*, s.region
    FROM with_dims e
    INNER JOIN stores s
        ON e.store_id = s.id
),
valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        revenue_per_unit,
        store_name,
        region,
        SUBSTRING(store_name, 1, 4) store_name_prefix
    FROM joined_store_sales
    WHERE revenue_per_unit >= 75
      AND sale_date >= TO_DATE('2023-04-01')
)
SELECT
    id,
    store_id,
    product_id,
    revenue,
    region,
    store_name_prefix
FROM valid
ORDER BY product_id DESC;
