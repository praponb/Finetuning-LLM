WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS tier_value
)
-- Window metrics over several different partitions
SELECT
    *,
    RANK() OVER w_grp AS tier_value_rank,
    PERCENT_RANK() OVER w_grp AS tier_value_percent_rank
FROM lines
WINDOW w_grp AS (ORDER BY tier_value DESC NULLS LAST);
