WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element
),
enriched AS (
    SELECT
        id,
        currency,
        RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS FIRST) AS element_rank,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_rank_2,
        PERCENT_RANK() OVER (ORDER BY element NULLS LAST) AS element_percent_rank
    FROM exploded
    WHERE element >= 31
),
metrics AS (
    SELECT
        *,
        ROUND(element_rank_2 / SUM(element_rank_2) OVER (PARTITION BY currency), 4) AS element_rank_2_share,
        ROUND(element_rank_2 / SUM(element_rank_2) OVER (PARTITION BY currency), 4) AS element_rank_2_share_2
    FROM enriched
),
scored AS (
    SELECT *, PERCENT_RANK() OVER w_grp AS element_rank_2_share_percent_rank
    FROM metrics
    WINDOW w_grp AS (ORDER BY element_rank_2_share NULLS LAST)
)
SELECT *
FROM (
    SELECT
        currency,
        element_rank_2_share,
        element_percent_rank,
        element_rank_2_share_percent_rank,
        element_rank_2_share_2
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (element_rank_2_share, element_percent_rank, element_rank_2_share_percent_rank, element_rank_2_share_2)
);
