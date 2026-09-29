WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(effective_from, 42) AS effective_from_minus_42d,
        CASE WHEN currency IN ('THB', 'USD') THEN 'Tier 1' ELSE 'Other' END AS currency_group
    FROM lines
    WHERE effective_from > DATE '2024-04-09'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        RANK() OVER w_recent AS val_rank,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
        PERCENT_RANK() OVER w_recent_2 AS val_percent_rank
    FROM filtered
    WHERE val > 51
    WINDOW w_recent AS (PARTITION BY DATE_TRUNC('month', effective_from_minus_42d), currency ORDER BY val DESC), w_recent_2 AS (PARTITION BY currency ORDER BY val DESC NULLS LAST)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(val_cume_dist, val_percent_rank) AS least_val_cume_dist_val_percent_rank,
        ROUND(val_cume_dist * 0.85, 2) AS adjusted_val_cume_dist
    FROM metrics
    WHERE effective_from > DATE '2024-05-06'
      AND currency = 'USD'
)
-- Aggregate with conditional metrics
SELECT
    currency,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    SUM(CASE WHEN currency = 'THB' THEN val_cume_dist ELSE 0 END) AS thb_val_cume_dist,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(DISTINCT currency) AS distinct_currencies,
    COUNT_IF(currency = 'USD') AS usd_events
FROM cleaned
GROUP BY currency
ORDER BY thb_val_cume_dist
