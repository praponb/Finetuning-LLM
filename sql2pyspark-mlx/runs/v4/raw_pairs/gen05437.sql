WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(element) OVER (PARTITION BY currency) AS currency_sum_element,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank,
        AVG(element) OVER (PARTITION BY currency) AS currency_avg_element
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_avg_element NULLS LAST) AS currency_avg_element_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_percent_rank,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency), 4) AS element_dense_rank_share
    FROM metrics
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, currency_sum_element + element_dense_rank AS currency_sum_element_plus_element_dense_rank
    FROM scored
    WHERE effective_from > TO_DATE('2024-05-23')
)
SELECT
    id,
    element_dense_rank,
    currency_avg_element_cume_dist,
    element_percent_rank,
    currency_sum_element_plus_element_dense_rank
FROM valid
ORDER BY currency_sum_element_plus_element_dense_rank NULLS LAST, element_dense_rank DESC
