WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY element DESC) AS element_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_percent_rank,
        MIN(element) OVER (PARTITION BY plan) AS plan_min_element,
        MAX(element) OVER (PARTITION BY currency, plan) AS currency_max_element
    FROM flattened
),
with_windows AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY element_rank) AS element_rank_rank,
        CUME_DIST() OVER (ORDER BY element_percent_rank NULLS FIRST) AS element_percent_rank_cume_dist
    FROM metrics
),
filtered AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element_rank,
        element_percent_rank,
        plan_min_element,
        currency_max_element,
        element_rank_rank,
        element_percent_rank_cume_dist,
        CAST(element_rank AS STRING) AS element_rank_str
    FROM with_windows
    WHERE effective_from >= DATE '2024-07-19'
),
cleaned AS (
    SELECT
        currency,
        element_percent_rank,
        plan_min_element,
        currency_max_element,
        element_percent_rank_cume_dist,
        element_rank_str,
        CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END AS is_enterprise
    FROM filtered
    WHERE element_percent_rank IS NOT NULL
)
SELECT
    CASE WHEN element_percent_rank >= 0.89 THEN 'LARGE' ELSE 'SMALL' END AS element_percent_rank_tier,
    currency,
    SUM(CASE WHEN currency = 'EUR' THEN element_percent_rank_cume_dist ELSE 0 END) AS eur_element_percent_rank_cume_dist,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT_IF(currency = 'EUR') AS eur_events,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows
FROM cleaned
GROUP BY CASE WHEN element_percent_rank >= 0.89 THEN 'LARGE' ELSE 'SMALL' END, currency
HAVING eur_events > 1;
