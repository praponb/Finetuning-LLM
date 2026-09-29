WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS tier_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
        RANK() OVER (PARTITION BY YEAR(effective_from), currency ORDER BY tier_value NULLS FIRST) AS tier_value_rank_2
    FROM lines
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        currency,
        tier_value_rank_2,
        ROUND(tier_value_rank_2 / SUM(tier_value_rank_2) OVER (PARTITION BY currency), 4) AS tier_value_rank_2_share,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value_rank_2 NULLS LAST) AS tier_value_rank_2_cume_dist
    FROM with_windows
    WHERE tier_value <= 22
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        tier_value_rank_2_share,
        tier_value_rank_2_cume_dist,
        LENGTH(currency) AS currency_length
    FROM enriched
    WHERE currency IN ('EUR', 'THB')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        currency,
        tier_value_rank_2_share,
        tier_value_rank_2_cume_dist,
        currency_length,
        CAST(tier_value_rank_2_cume_dist AS INT) AS tier_value_rank_2_cume_dist_int
    FROM base
    WHERE currency_length <= 56
),
with_windows_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(tier_value_rank_2_cume_dist_int) OVER w_running AS currency_sum_tier_value_rank_2_cume_dist_int,
        CUME_DIST() OVER w_part AS tier_value_rank_2_share_cume_dist,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_rank_2_share DESC NULLS FIRST) AS tier_value_rank_2_share_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_rank_2_cume_dist_int DESC NULLS LAST) AS tier_value_rank_2_cume_dist_int_dense_rank,
        MIN(currency_length) OVER w_running AS currency_min_currency_length
    FROM cleaned
    WINDOW w_running AS (PARTITION BY currency), w_part AS (ORDER BY tier_value_rank_2_share NULLS FIRST)
)
SELECT
    id,
    currency,
    tier_value_rank_2_share,
    tier_value_rank_2_cume_dist,
    currency_length,
    tier_value_rank_2_cume_dist_int,
    currency_sum_tier_value_rank_2_cume_dist_int,
    tier_value_rank_2_share_cume_dist,
    tier_value_rank_2_cume_dist_int_dense_rank
FROM with_windows_price_tiers
ORDER BY currency_length, currency DESC;
