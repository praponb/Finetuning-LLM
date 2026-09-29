WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_dense_rank_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_cume_dist
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(element) OVER w_running AS currency_avg_element,
        RANK() OVER w_recent AS element_cume_dist_rank,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency), 4) AS element_dense_rank_share
    FROM metrics
    WINDOW w_running AS (PARTITION BY currency), w_recent AS (PARTITION BY currency ORDER BY element_cume_dist DESC NULLS LAST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, element_dense_rank_share / NULLIF(element_cume_dist, 0) AS element_dense_rank_share_per_element_cume_dist
    FROM with_windows
    WHERE element_cume_dist > 0.72
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(element_dense_rank_2) OVER w_grp AS currency_count_element_dense_rank_2,
        CUME_DIST() OVER (ORDER BY element_cume_dist DESC NULLS LAST) AS element_cume_dist_cume_dist,
        STDDEV_SAMP(element_dense_rank_share) OVER w_grp AS currency_stddev_samp_element_dense_rank_share,
        RANK() OVER (PARTITION BY currency ORDER BY element_cume_dist_rank DESC NULLS FIRST) AS element_cume_dist_rank_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY currency)
)
SELECT
    currency,
    element,
    element_dense_rank,
    element_dense_rank_2,
    element_cume_dist_rank,
    element_dense_rank_share,
    currency_count_element_dense_rank_2,
    element_cume_dist_cume_dist,
    currency_stddev_samp_element_dense_rank_share
FROM scored;
