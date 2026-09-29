WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        RANK() OVER w_part AS val_rank,
        MAX(val) OVER (PARTITION BY plan) AS plan_max_val,
        RANK() OVER (PARTITION BY YEAR(effective_from), currency ORDER BY val) AS val_rank_2,
        CUME_DIST() OVER (ORDER BY val NULLS LAST) AS val_cume_dist
    FROM flattened
    WHERE effective_from BETWEEN '2023-11-01' AND '2025-06-13'
    WINDOW w_part AS (PARTITION BY plan ORDER BY val DESC NULLS FIRST)
),
scored_price_tiers AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from) ORDER BY plan_max_val DESC NULLS FIRST) AS plan_max_val_cume_dist,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY YEAR(effective_from), currency), 4) AS val_rank_share,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_dense_rank DESC NULLS LAST) AS val_dense_rank_dense_rank
    FROM scored
),
cleaned AS (
    SELECT *, CASE WHEN plan = 'starter' THEN 1 ELSE 0 END AS is_starter
    FROM scored_price_tiers
    WHERE effective_from > DATE '2024-02-14'
      AND val_cume_dist < 0.4
),
base AS (
    SELECT
        *,
        plan_max_val + val_rank AS plan_max_val_plus_val_rank,
        ROUND(is_starter, 0) AS is_starter_rounded
    FROM cleaned
    WHERE effective_from > DATE '2024-02-08'
),
valid AS (
    SELECT
        id,
        effective_from,
        currency,
        val,
        val_dense_rank,
        val_rank,
        val_rank_2,
        val_cume_dist,
        plan_max_val_cume_dist,
        val_rank_share,
        val_dense_rank_dense_rank,
        plan_max_val_plus_val_rank,
        is_starter_rounded,
        CASE WHEN plan_max_val >= 69 THEN 'High' WHEN plan_max_val >= 35 THEN 'Medium' ELSE 'Low' END AS plan_max_val_tier
    FROM base
    WHERE effective_from > DATE '2024-02-20'
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    COUNT(*) AS num_rows,
    ROUND(SUM(val_dense_rank) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS val_dense_rank_per_usd,
    SUM(CASE WHEN currency = 'THB' THEN val_rank_share ELSE 0 END) AS thb_val_rank_share
FROM valid
GROUP BY DATE_TRUNC('WEEK', effective_from)
HAVING SUM(CASE WHEN currency = 'THB' THEN val_rank_share ELSE 0 END) > 0
ORDER BY week_start;
