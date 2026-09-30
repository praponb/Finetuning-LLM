WITH metrics AS (
    SELECT
        *,
        COUNT(rating) OVER w_ordered AS last4_count_rating,
        MAX(rating) OVER w_ordered AS last4_max_rating,
        AVG(rating) OVER w_ordered AS last4_avg_rating,
        MIN(rating) OVER w_ordered AS last4_min_rating
    FROM suppliers
    WHERE rating <= 3.3
    WINDOW w_ordered AS (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics_suppliers AS (
    SELECT
        id,
        supplier_name,
        city,
        last4_avg_rating,
        NTILE(3) OVER (PARTITION BY city, country ORDER BY last4_min_rating, id) AS last4_min_rating_bucket,
        NTILE(3) OVER (ORDER BY last4_count_rating, id) AS last4_count_rating_bucket,
        ROUND(last4_max_rating / SUM(last4_max_rating) OVER (PARTITION BY country), 4) AS last4_max_rating_share,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY last4_min_rating DESC) AS last4_min_rating_dense_rank
    FROM metrics
),
with_last AS (
    SELECT s.*, top1.product_launch_date
    FROM metrics_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY last4_count_rating_bucket DESC NULLS FIRST, id) AS latest_rank
    FROM with_last
    QUALIFY latest_rank = 1
),
metrics_suppliers_2 AS (
    SELECT
        *,
        LAG(last4_min_rating_bucket) OVER (PARTITION BY city ORDER BY product_launch_date, id) AS prev_last4_min_rating_bucket,
        LEAD(last4_min_rating_bucket) OVER (PARTITION BY city ORDER BY product_launch_date, id) AS next_last4_min_rating_bucket,
        LEAD(last4_count_rating_bucket) OVER (PARTITION BY city ORDER BY product_launch_date, id) AS next_last4_count_rating_bucket,
        LAG(last4_count_rating_bucket) OVER (PARTITION BY city ORDER BY product_launch_date, id) AS prev_last4_count_rating_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_launch_date, id) AS seq_no,
        ROUND(last4_count_rating_bucket / SUM(last4_count_rating_bucket) OVER (PARTITION BY DATE_TRUNC('YEAR', product_launch_date), city), 4) AS last4_count_rating_bucket_share
    FROM latest
),
summary AS (
    SELECT
        city,
        ROUND(AVG(prev_last4_count_rating_bucket), 2) AS avg_prev_last4_count_rating_bucket,
        SUM(CASE WHEN city = 'Berlin' THEN last4_min_rating_bucket ELSE 0 END) AS berlin_last4_min_rating_bucket,
        ROUND(SUM(prev_last4_count_rating_bucket) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS prev_last4_count_rating_bucket_per_chiang_mai,
        COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
        SUM(last4_count_rating_bucket_share) AS total_last4_count_rating_bucket_share
    FROM metrics_suppliers_2
    GROUP BY city
)
SELECT
    city,
    avg_prev_last4_count_rating_bucket,
    london_count,
    total_last4_count_rating_bucket_share
FROM summary
ORDER BY city DESC, total_last4_count_rating_bucket_share DESC
