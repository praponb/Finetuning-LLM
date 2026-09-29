WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(supplier_name, ', ', city) AS supplier_name_city
    FROM suppliers
    WHERE country IN ('US', 'TH', 'DE')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand = 'Initech'
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_launch_date
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC, id) AS rank_in_group
    FROM with_last
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(rating) OVER w_grp AS prev_rating,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM ranked
    WINDOW w_grp AS (PARTITION BY city ORDER BY rank_in_group, id)
)
SELECT
    id,
    supplier_name,
    country,
    supplier_name_city,
    product_launch_date
FROM metrics;
