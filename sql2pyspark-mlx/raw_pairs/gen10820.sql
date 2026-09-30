WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        currency,
        val,
        UPPER(plan) AS plan_upper,
        CASE WHEN val >= 79 THEN 'High' WHEN val >= 16 THEN 'Medium' ELSE 'Low' END AS val_level
    FROM flattened
    WHERE effective_from >= DATE '2024-03-11'
      AND effective_from BETWEEN '2024-03-01' AND '2024-12-14'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(effective_from, 6) AS effective_from_plus_months
    FROM cleaned
    WHERE effective_from > DATE '2024-04-13'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER (PARTITION BY currency ORDER BY val ASC NULLS LAST) AS val_cume_dist
    FROM filtered
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        currency,
        plan_upper,
        val_level,
        effective_from_plus_months,
        val_cume_dist,
        ROUND(val, 1) AS val_rounded
    FROM enriched
    WHERE effective_from BETWEEN '2024-06-01' AND '2024-06-21'
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', effective_from_plus_months) AS activity_month,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
        SIZE(COLLECT_SET(currency)) AS num_currencies,
        SUM(CASE WHEN currency = 'EUR' THEN val_cume_dist ELSE 0 END) AS eur_val_cume_dist,
        COUNT_IF(currency = 'THB') AS thb_events
    FROM cleaned_price_tiers
    GROUP BY DATE_TRUNC('MONTH', effective_from_plus_months)
    HAVING SIZE(COLLECT_SET(currency)) >= 1
)
SELECT
    activity_month,
    currency_list,
    num_currencies,
    eur_val_cume_dist
FROM grouped
ORDER BY num_currencies DESC NULLS FIRST, activity_month DESC
LIMIT 5
