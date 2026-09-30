WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS val
),
scored AS (
    -- Ranking and gap features
    SELECT *, MAX(val) OVER (PARTITION BY currency) AS currency_max_val
    FROM exploded
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY MONTH(effective_from), currency ORDER BY val NULLS FIRST) AS val_cume_dist
    FROM scored
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        currency,
        val,
        val_cume_dist,
        RANK() OVER (PARTITION BY YEAR(effective_from), currency ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_rank
    FROM scored_price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(val_cume_dist AS INT) AS val_cume_dist_int,
        ROUND(val * 100, 2) AS adjusted_val
    FROM enriched
    WHERE effective_from >= TO_DATE('2024-05-16')
      AND currency NOT IN ('USD', 'THB')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(val_cume_dist_int) OVER (PARTITION BY currency) AS currency_sum_val_cume_dist_int,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_cume_dist DESC) AS val_cume_dist_dense_rank
    FROM base
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        val_cume_dist_rank,
        val_cume_dist_int,
        adjusted_val,
        currency_sum_val_cume_dist_int,
        val_cume_dist_dense_rank,
        val + val_cume_dist_rank AS val_plus_val_cume_dist_rank
    FROM with_windows
    WHERE effective_from >= TO_DATE('2024-01-23')
      AND val_cume_dist > 0.26
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN val_cume_dist_int >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_cume_dist_int_tier,
        PERCENTILE_APPROX(val_cume_dist_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS val_cume_dist_dense_rank_quartiles,
        PERCENTILE_APPROX(currency_sum_val_cume_dist_int, 0.9) AS currency_sum_val_cume_dist_int_p90,
        APPROX_PERCENTILE(val_cume_dist_rank, 0.5) AS val_cume_dist_rank_p50,
        PERCENTILE_APPROX(currency_sum_val_cume_dist_int, 0.9) AS currency_sum_val_cume_dist_int_p90_2,
        ROUND(AVG(currency_sum_val_cume_dist_int), 2) AS avg_currency_sum_val_cume_dist_int
    FROM cleaned
    GROUP BY val_cume_dist_int_tier
)
SELECT
    val_cume_dist_int_tier,
    val_cume_dist_dense_rank_quartiles,
    currency_sum_val_cume_dist_int_p90,
    val_cume_dist_rank_p50,
    currency_sum_val_cume_dist_int_p90_2,
    avg_currency_sum_val_cume_dist_int
FROM summary
ORDER BY val_cume_dist_rank_p50;
