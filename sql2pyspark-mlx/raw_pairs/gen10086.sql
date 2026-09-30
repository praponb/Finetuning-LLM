WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        pos,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) x AS pos, elem
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY promo ORDER BY price DESC, id, pos) AS price_bucket,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS LAST) AS price_dense_rank
    FROM flattened
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        pos,
        qty,
        price,
        price_dense_rank
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo ORDER BY price_dense_rank DESC NULLS LAST, id, pos) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        pos,
        price,
        price_dense_rank,
        SUM(qty) OVER (ORDER BY price, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        LEAD(price, 1, 0) OVER w_running AS next_price,
        PERCENT_RANK() OVER w_part AS price_dense_rank_percent_rank
    FROM latest
    WINDOW w_running AS (ORDER BY price_dense_rank, id, pos), w_part AS (ORDER BY price_dense_rank DESC NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_price >= 246 THEN 'LARGE' ELSE 'SMALL' END AS next_price_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(price_dense_rank), 2) AS avg_price_dense_rank,
    PERCENTILE_APPROX(price, 0.9) AS price_p90,
    PERCENTILE_APPROX(pos, 0.95) AS pos_p95,
    PERCENTILE_APPROX(price_dense_rank, 0.9) AS price_dense_rank_p90,
    COUNT(*) AS num_rows
FROM scored
GROUP BY CASE WHEN next_price >= 246 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY next_price_tier NULLS LAST;
