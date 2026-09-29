WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.signup_date,
        p.credit_limit
    FROM exploded src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(credit_limit) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS credit_limit_count_1h,
        DENSE_RANK() OVER (PARTITION BY customer_id, code ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank
    FROM with_dims
),
scored AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        code,
        DENSE_RANK() OVER w_hist AS credit_limit_dense_rank_2,
        COUNT(credit_limit_count_1h) OVER (PARTITION BY code, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS credit_limit_count_1h_count_15m,
        PERCENT_RANK() OVER (PARTITION BY code ORDER BY credit_limit_dense_rank DESC NULLS LAST) AS credit_limit_dense_rank_percent_rank
    FROM enriched
    WINDOW w_hist AS (PARTITION BY code ORDER BY credit_limit DESC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit_dense_rank_percent_rank >= 0.89 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_dense_rank_percent_rank_tier,
    CASE WHEN credit_limit_dense_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_dense_rank_2_tier,
    SIZE(COLLECT_SET(code)) AS num_codes,
    ROUND(SUM(credit_limit_dense_rank_2) / NULLIF(COUNT(CASE WHEN code = 'VIP' THEN 1 END), 0), 2) AS credit_limit_dense_rank_2_per_vip,
    ROUND(AVG(credit_limit_dense_rank_2), 2) AS avg_credit_limit_dense_rank_2
FROM scored
GROUP BY CASE WHEN credit_limit_dense_rank_percent_rank >= 0.89 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN credit_limit_dense_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END
ORDER BY credit_limit_dense_rank_2_per_vip DESC
