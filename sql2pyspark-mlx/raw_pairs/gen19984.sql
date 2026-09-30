WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY country ORDER BY rating, id) AS rating_bucket,
        LAG(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating
    FROM suppliers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        rating,
        prev_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        NTILE(5) OVER (ORDER BY rating DESC, id) AS rating_bucket_2,
        AVG(rating_bucket) OVER (PARTITION BY city ORDER BY prev_rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_bucket_avg_5
    FROM metrics
    WHERE country IN ('TH', 'JP')
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_launch_date,
        top1.stock_qty
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        prev_rating,
        stock_qty,
        SUM(stock_qty) OVER w_grp AS running_stock_qty,
        AVG(rating_bucket_avg_5) OVER w_hist AS rating_bucket_avg_5_avg_2,
        AVG(rating_share) OVER w_grp_2 AS rating_share_avg_3_prior
    FROM with_last
    WINDOW w_grp AS (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY prev_rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_grp_2 AS (ORDER BY prev_rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    prev_rating,
    stock_qty,
    rating_share_avg_3_prior
FROM scored
ORDER BY prev_rating DESC NULLS FIRST
