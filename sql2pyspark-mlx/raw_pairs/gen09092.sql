WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-05-19'
      AND revenue >= 1900
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        revenue_per_unit,
        LAG(revenue, 2) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id) AS prev_revenue,
        FIRST_VALUE(revenue_per_unit, TRUE) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue_per_unit,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY revenue_per_unit NULLS FIRST) AS revenue_per_unit_cume_dist
    FROM base
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.sale_date,
        src.revenue_per_unit,
        src.revenue_per_unit_cume_dist,
        s.city,
        s.store_name,
        s.region
    FROM metrics src
    LEFT JOIN stores s
        ON src.store_id = s.id
    WHERE s.city = 'New York'
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.sale_date,
        s.revenue_per_unit_cume_dist,
        s.city,
        b.store_name AS store_store_name,
        b.city AS store_city
    FROM joined s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
base_store_sales AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        revenue_per_unit_cume_dist,
        store_city,
        TRIM(store_store_name) AS clean_store_store_name,
        UPPER(city) AS city_upper,
        COALESCE(store_city, 'UNKNOWN') AS store_city_clean
    FROM with_dims
    WHERE store_id IN (1, 2, 4)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_city ORDER BY revenue_per_unit_cume_dist DESC NULLS FIRST, id) AS row_num
    FROM base_store_sales
    QUALIFY row_num = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_city, store_id ORDER BY revenue_per_unit_cume_dist NULLS LAST, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    store_id,
    clean_store_store_name,
    store_city_clean,
    row_num
FROM deduped;
