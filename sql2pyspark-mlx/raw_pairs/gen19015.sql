WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS tier_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from,
        currency,
        CASE WHEN currency = 'USD' THEN 1 ELSE 0 END AS is_usd,
        INITCAP(TRIM(currency)) AS currency_title
    FROM exploded
    WHERE tier_value < 28
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY is_usd DESC NULLS LAST) AS is_usd_cume_dist
    FROM cleaned
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        effective_from,
        currency,
        is_usd,
        currency_title,
        is_usd_cume_dist,
        CASE WHEN is_usd >= 66 THEN 'High' WHEN is_usd >= 23 THEN 'Medium' ELSE 'Low' END AS is_usd_band,
        CASE WHEN currency IN ('USD', 'THB') THEN 'Priority' ELSE 'Other' END AS currency_group
    FROM with_windows
    WHERE effective_from > TO_DATE('2024-05-21')
)
SELECT
    id,
    plan,
    effective_from,
    currency_title,
    is_usd_cume_dist,
    is_usd_band
FROM base;
