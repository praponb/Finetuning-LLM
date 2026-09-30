WITH flattened AS (
    SELECT
        id,
        effective_from,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    effective_from,
    tier_value,
    RANK() OVER w_grp AS tier_value_rank,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
    COUNT(tier_value) OVER (PARTITION BY YEAR(effective_from)) AS effective_from_year_count_tier_value
FROM flattened
WHERE YEAR(effective_from) = 2024
WINDOW w_grp AS (ORDER BY tier_value);
