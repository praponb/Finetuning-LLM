WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        c.country,
        c.age
    FROM flattened a
    INNER JOIN customers c
        ON a.customer_id = c.id
    WHERE c.city IN ('Tokyo', 'London')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        DENSE_RANK() OVER (ORDER BY age DESC NULLS FIRST) AS age_dense_rank,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY age ASC) AS age_dense_rank_2,
        ROUND(age / SUM(age) OVER (PARTITION BY customer_id), 4) AS age_share_2,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY age ASC) AS age_percent_rank
    FROM with_dims
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        age_share,
        age_dense_rank_2,
        age_share_2,
        age_percent_rank,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY age_share_2 DESC NULLS LAST) AS age_share_2_cume_dist
    FROM with_windows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        age_share,
        age_dense_rank_2,
        age_percent_rank,
        age_share_2_cume_dist,
        ROUND(age_percent_rank / age_dense_rank_2, 2) AS age_percent_rank_per_age_dense_rank_2
    FROM enriched
    WHERE age_dense_rank_2 >= 5
)
-- Grouped metrics
SELECT
    CASE WHEN age_percent_rank_per_age_dense_rank_2 >= 82 THEN 'LARGE' ELSE 'SMALL' END AS age_percent_rank_per_age_dense_rank_2_tier,
    CASE WHEN age_share >= 0.89 THEN 'LARGE' ELSE 'SMALL' END AS age_share_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(age_share, 0.5) AS age_share_p50,
    PERCENTILE_APPROX(age_share_2_cume_dist, 0.5) AS age_share_2_cume_dist_p50,
    COUNT(*) AS cnt_2,
    GROUPING_ID() AS grouping_id
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN age_percent_rank_per_age_dense_rank_2 >= 82 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN age_share >= 0.89 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN age_percent_rank_per_age_dense_rank_2 >= 82 THEN 'LARGE' ELSE 'SMALL' END))
