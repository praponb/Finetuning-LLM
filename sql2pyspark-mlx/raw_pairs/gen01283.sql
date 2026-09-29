WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(brand, 1, 3) brand_prefix,
        price * cost margin
    FROM products
    WHERE launch_date > TO_DATE('2023-04-19')
      AND brand IS NOT NULL
),
valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        brand_prefix,
        margin,
        TRIM(product_name) clean_product_name,
        COALESCE(brand, 'DIRECT') resolved_brand
    FROM cleaned
    WHERE launch_date >= TO_DATE('2023-06-30')
      AND brand IS NOT NULL
),
with_dims AS (
    SELECT
        a.id,
        a.product_name,
        a.cost,
        a.stock_qty,
        a.tags,
        a.supplier_id,
        a.brand_prefix,
        a.resolved_brand,
        p.supplier_name,
        p.rating
    FROM valid a
    INNER JOIN suppliers p
        ON a.supplier_id <=> p.id
),
metrics AS (
    SELECT
        *,
        AVG(rating) OVER w_grp running_avg_rating,
        SUM(rating) OVER w_grp running_sum_rating,
        COUNT(rating) OVER w_grp running_count_rating,
        MAX(rating) OVER w_grp running_max_rating,
        LEAD(stock_qty, 2) OVER w_grp_2 next_stock_qty
    FROM with_dims
    WHERE supplier_name LIKE 'Pa%'
    WINDOW w_grp AS (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (ORDER BY stock_qty, id)
),
deduped AS (
    SELECT
        id,
        product_name,
        resolved_brand,
        supplier_name,
        rating,
        running_count_rating,
        running_max_rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY running_count_rating, id) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
metrics_products AS (
    SELECT
        id,
        supplier_name,
        running_count_rating,
        ROW_NUMBER() OVER (ORDER BY running_count_rating, id) event_seq,
        LEAD(running_max_rating, 1) OVER (ORDER BY rating, id) next_running_max_rating
    FROM deduped
),
with_latest AS (
    SELECT
        s.id,
        s.supplier_name,
        s.running_count_rating,
        s.event_seq,
        s.next_running_max_rating,
        latest.sale_date
    FROM metrics_products s
    LEFT JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    PERCENTILE_APPROX(next_running_max_rating, ARRAY(0.25, 0.5, 0.75)) AS next_running_max_rating_quartiles,
    PERCENTILE_APPROX(next_running_max_rating, 0.9) AS next_running_max_rating_p90
FROM with_latest
GROUP BY year_start, quarter_start;
