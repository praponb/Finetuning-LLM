WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ARRAY_CONTAINS(ch.tags, 'eco')
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY city, country ORDER BY rating DESC, id) rating_bucket,
        RANK() OVER (ORDER BY rating NULLS FIRST) rating_rank,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) rating_dense_rank,
        AVG(rating) OVER (PARTITION BY city, country) city_avg_rating
    FROM kept
    WHERE country IN ('TH', 'JP', 'DE')
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_category
    FROM scored s
    JOIN LATERAL (
            SELECT c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.rating,
        s.rating_bucket,
        s.rating_dense_rank,
        s.city_avg_rating,
        s.product_category,
        latest.product_stock_qty
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Running and moving aggregates
SELECT
    id,
    rating,
    rating_bucket,
    rating_dense_rank,
    product_category,
    product_stock_qty,
    LAG(product_stock_qty) OVER w_recent prev_product_stock_qty,
    LEAD(product_stock_qty) OVER w_recent next_product_stock_qty,
    LAG(rating_bucket) OVER w_recent prev_rating_bucket,
    LEAD(rating_bucket) OVER w_recent next_rating_bucket,
    ROW_NUMBER() OVER w_recent seq_no,
    NTILE(4) OVER (ORDER BY rating DESC, id) rating_bucket_2
FROM with_latest
WINDOW w_recent AS (PARTITION BY product_category ORDER BY rating, id)
