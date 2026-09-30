WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.product_name,
        e.brand,
        e.price,
        e.launch_date,
        e.tags,
        e.supplier_id,
        b.supplier_name,
        b.rating,
        b.city
    FROM products e
    LEFT JOIN suppliers b
        ON e.supplier_id <=> b.id
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.brand,
        e.price,
        e.supplier_id,
        e.supplier_name,
        e.rating,
        e.city,
        b.city AS supplier_city,
        b.country,
        b.supplier_name AS supplier_supplier_name
    FROM with_dims e
    LEFT JOIN suppliers b
        ON e.supplier_id <=> b.id
    WHERE b.country IN ('JP', 'UK', 'TH')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(rating) OVER (PARTITION BY supplier_id ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating,
        AVG(rating) OVER (PARTITION BY supplier_id ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_rating,
        COUNT(rating) OVER (PARTITION BY supplier_id ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_rating
    FROM with_dims_products
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        brand,
        price,
        supplier_name,
        rating,
        city,
        supplier_city,
        supplier_supplier_name,
        running_max_rating,
        running_avg_rating,
        ROW_NUMBER() OVER (PARTITION BY supplier_city ORDER BY rating, id) AS event_seq,
        LEAD(price, 2) OVER (PARTITION BY supplier_id ORDER BY running_avg_rating, id) AS next_price
    FROM with_windows
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_sale_date
    FROM metrics s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    supplier_name,
    rating,
    city,
    supplier_city,
    supplier_supplier_name,
    running_max_rating,
    running_avg_rating,
    event_seq
FROM with_last
ORDER BY id NULLS FIRST
LIMIT 10;
