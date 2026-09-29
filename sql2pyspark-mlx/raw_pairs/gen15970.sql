WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        REGEXP_REPLACE(product_name, ' ', '') AS product_name_slug,
        LAST_DAY(launch_date) AS launch_date_month_end,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE cost IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cost) OVER w_part AS prev_cost,
        LEAD(price) OVER w_part AS next_price,
        LAG(price) OVER w_part AS prev_price,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM cleaned
    WINDOW w_part AS (PARTITION BY brand ORDER BY price, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(seq_no) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_seq_no,
        LEAD(prev_cost) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_prev_cost,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date, id) AS seq_no_2,
        AVG(prev_cost) OVER (PARTITION BY category, brand ORDER BY launch_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS prev_cost_avg_5_prior
    FROM enriched
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.price,
        src.launch_date,
        src.tags,
        src.supplier_id,
        src.prev_price,
        src.seq_no,
        src.next_seq_no,
        src.next_prev_cost,
        src.prev_cost_avg_5_prior,
        p.supplier_name,
        p.city,
        p.country
    FROM metrics src
    LEFT JOIN suppliers p
        ON src.supplier_id <=> p.id
),
cleaned_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(city) AS city_upper,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM enriched_products
    WHERE brand IS NOT NULL
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.price,
        src.tags,
        src.supplier_id,
        src.prev_price,
        src.seq_no,
        src.next_seq_no,
        src.next_prev_cost,
        src.city,
        p.city AS supplier_city,
        p.supplier_name AS supplier_supplier_name,
        p.country AS supplier_country
    FROM cleaned_products src
    LEFT JOIN suppliers p
        ON src.supplier_id <=> p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_city,
        supplier_id,
        city,
        price
    FROM joined
) src
PIVOT (
    MAX(price) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('Singapore', 'Tokyo', 'London')
);
