WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
),
with_windows AS (
    SELECT *, RANK() OVER (PARTITION BY DATE_TRUNC('week', effective_from), currency ORDER BY val NULLS LAST) AS val_rank
    FROM lines
),
with_windows_price_tiers AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_rank DESC NULLS LAST) AS val_rank_cume_dist,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY DATE_TRUNC('month', effective_from), plan), 4) AS val_rank_share
    FROM with_windows
    WHERE currency IN ('THB', 'USD')
),
with_windows_price_tiers_2 AS (
    SELECT
        plan,
        effective_from,
        val,
        val_rank,
        val_rank_share,
        MAX(val_rank_share) OVER (PARTITION BY plan, currency) AS plan_max_val_rank_share,
        PERCENT_RANK() OVER (ORDER BY val) AS val_percent_rank
    FROM with_windows_price_tiers
)
SELECT
    effective_from,
    plan_max_val_rank_share,
    val_percent_rank
FROM with_windows_price_tiers_2
