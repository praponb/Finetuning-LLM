WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS element
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        element,
        STDDEV_SAMP(element) OVER (PARTITION BY currency) AS currency_stddev_samp_element,
        DENSE_RANK() OVER w_grp AS element_dense_rank
    FROM lines
    WINDOW w_grp AS (PARTITION BY currency ORDER BY element DESC)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        element,
        currency_stddev_samp_element,
        element_dense_rank,
        PERCENT_RANK() OVER (ORDER BY element_dense_rank NULLS LAST) AS element_dense_rank_percent_rank,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency, plan), 4) AS element_dense_rank_share,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_percent_rank,
        STDDEV_SAMP(element) OVER (PARTITION BY currency, plan) AS currency_stddev_samp_element_2
    FROM scored
)
-- Window metrics over several different partitions
SELECT
    id,
    plan,
    currency,
    element,
    currency_stddev_samp_element,
    element_dense_rank,
    element_dense_rank_percent_rank,
    element_dense_rank_share,
    element_percent_rank,
    currency_stddev_samp_element_2,
    RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_rank,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_element DESC) AS currency_stddev_samp_element_cume_dist,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_element DESC NULLS FIRST) AS currency_stddev_samp_element_dense_rank,
    DENSE_RANK() OVER w_recent AS element_dense_rank_2
FROM metrics
WINDOW w_recent AS (PARTITION BY currency ORDER BY element DESC);
