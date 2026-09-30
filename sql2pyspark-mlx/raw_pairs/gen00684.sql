WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(plan) AS plan_lower
    FROM exploded
    WHERE effective_from >= TO_DATE('2024-05-30')
      AND currency = 'USD'
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(plan, ', ', currency) AS plan_currency,
        CONCAT(plan, ', ', currency) AS plan_currency_2
    FROM cleaned
    WHERE val = 61
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN currency IN ('USD', 'EUR') THEN 'Tier 1' ELSE 'Other' END AS currency_group
    FROM cleaned_price_tiers
    WHERE effective_from >= DATE '2024-03-11'
),
kept_keys AS (
    -- Set difference on keys
    SELECT currency
    FROM filtered
    EXCEPT DISTINCT
    SELECT currency
    FROM price_tiers
    WHERE effective_from > '2024-02-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.currency = k.currency
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(val) OVER (PARTITION BY plan) AS plan_max_val,
        MAX(val) OVER w_running AS currency_max_val,
        PERCENT_RANK() OVER w_hist AS val_percent_rank
    FROM verified
    WINDOW w_running AS (PARTITION BY currency), w_hist AS (ORDER BY val NULLS FIRST)
)
-- Grouped metrics
SELECT
    currency,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    APPROX_PERCENTILE(currency_max_val, ARRAY(0.25, 0.5, 0.75)) AS currency_max_val_quartiles,
    COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count,
    ROUND(AVG(currency_max_val), 2) AS avg_currency_max_val,
    COUNT(*) AS cnt
FROM scored
GROUP BY currency
ORDER BY enterprise_count DESC;
