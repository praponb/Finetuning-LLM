WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS elem
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        product_id,
        qty,
        price,
        ROUND(price / SUM(price) OVER (PARTITION BY customer_id), 4) price_share,
        STDDEV_SAMP(price) OVER w_grp customer_id_stddev_samp_price,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY product_id DESC NULLS FIRST) product_id_cume_dist
    FROM flattened
    WINDOW w_grp AS (PARTITION BY customer_id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        product_id,
        qty,
        price,
        price_share,
        customer_id_stddev_samp_price,
        product_id_cume_dist,
        ROUND(price / SUM(price) OVER (PARTITION BY customer_id), 4) price_share_2,
        DENSE_RANK() OVER w_ordered qty_dense_rank,
        DENSE_RANK() OVER (ORDER BY product_id DESC NULLS FIRST) product_id_dense_rank
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY qty NULLS FIRST)
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    PERCENTILE_APPROX(product_id, 0.9) AS product_id_p90,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    PERCENTILE_APPROX(product_id_cume_dist, 0.9) AS product_id_cume_dist_p90
FROM scored
GROUP BY ROLLUP (customer_id)
HAVING distinct_customer_ids < 1;
