WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        li_item.product_id,
        li_item.qty,
        li_item.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS li_item
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM lines
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE customer_id = 29
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY price NULLS LAST) AS price_rank,
        CUME_DIST() OVER (ORDER BY price DESC NULLS FIRST) AS price_cume_dist
    FROM kept_rows
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_dense_rank,
        RANK() OVER (ORDER BY price_cume_dist NULLS FIRST) AS price_cume_dist_rank,
        DENSE_RANK() OVER (ORDER BY price_rank DESC) AS price_rank_dense_rank
    FROM enriched
),
scored_carts AS (
    -- Ranking and gap features
    SELECT
        product_id,
        qty,
        price,
        qty_dense_rank,
        price_cume_dist_rank,
        price_rank_dense_rank,
        CUME_DIST() OVER (ORDER BY price NULLS LAST) AS price_cume_dist_2,
        PERCENT_RANK() OVER (ORDER BY product_id NULLS LAST) AS product_id_percent_rank,
        CUME_DIST() OVER w_running AS price_cume_dist_3
    FROM scored
    WINDOW w_running AS (ORDER BY price DESC NULLS FIRST)
)
-- Grouped metrics
SELECT
    CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    MIN(price) AS min_price,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(price_cume_dist_2, 0.5) AS price_cume_dist_2_p50
FROM scored_carts
GROUP BY 1
HAVING APPROX_PERCENTILE(price_cume_dist_2, 0.5) < 1000000
ORDER BY price_cume_dist_2_p50;
