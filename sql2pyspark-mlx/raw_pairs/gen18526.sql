WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(currency)) AS currency_title,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM exploded
    WHERE currency != 'USD'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        currency_title,
        CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic
    FROM base
    WHERE effective_from > TO_DATE('2024-02-26')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        currency,
        currency_title,
        MAX(is_basic) OVER (PARTITION BY plan) AS plan_max_is_basic,
        MIN(val) OVER (PARTITION BY plan) AS plan_min_val,
        RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank
    FROM cleaned
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(effective_from) AS effective_from_month_end
    FROM metrics
    WHERE plan_max_is_basic < 31
)
-- Grouped metrics
SELECT
    currency,
    SUM(CASE WHEN currency = 'EUR' THEN plan_min_val ELSE 0 END) AS eur_plan_min_val,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    MAX(plan_max_is_basic) AS max_plan_max_is_basic
FROM filtered
GROUP BY currency
ORDER BY eur_plan_min_val;
