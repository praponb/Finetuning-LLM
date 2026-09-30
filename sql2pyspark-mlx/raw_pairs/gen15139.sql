WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(element) element_lower,
        CASE WHEN element IN ('VIP', 'BUNDLE') THEN 'Group A' ELSE 'Other' END element_group
    FROM exploded
    WHERE element = 'VIP'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.element,
        e.element_lower,
        e.element_group,
        p.segment,
        p.country,
        p.city
    FROM filtered e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        element_lower,
        element_group,
        country,
        city,
        CASE WHEN element = 'WELCOME5' THEN 1 ELSE 0 END is_welcome5
    FROM joined
    WHERE city IS NOT NULL
      AND element IS NOT DISTINCT FROM 'WELCOME5'
)
-- Grouped metrics
SELECT
    CASE WHEN is_welcome5 >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_welcome5_tier,
    ROUND(SUM(is_welcome5) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS is_welcome5_per_bangkok,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
FROM base
GROUP BY is_welcome5_tier;
