WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        COALESCE(supplier_name, 'Unknown') AS supplier_name_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_seq,
        LAST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST) AS rating_rank
    FROM filtered
    WHERE rating < 2.0
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_rank DESC NULLS FIRST, id) = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.supplier_name_filled,
        s.row_seq,
        s.last_known_rating,
        s.rating_rank,
        x.product_brand,
        x.stock_qty
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_brand ORDER BY rating_rank DESC NULLS LAST, id) AS dedup_rank
    FROM with_latest
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    product_brand,
    SORT_ARRAY(COLLECT_SET(product_brand)) AS product_brand_list,
    SUM(last_known_rating) AS total_last_known_rating,
    PERCENTILE_APPROX(stock_qty, 0.9) AS stock_qty_p90
FROM latest
GROUP BY product_brand
HAVING stock_qty_p90 > 0
