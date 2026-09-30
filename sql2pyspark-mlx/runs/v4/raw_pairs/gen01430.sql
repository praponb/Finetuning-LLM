WITH remaining_keys AS (
    SELECT country, browser
    FROM sessions
    EXCEPT DISTINCT
    SELECT country, browser
    FROM sessions
    WHERE country != 'JP'
),
reconciled AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.browser = k.browser
),
base AS (
    SELECT *, LOWER(browser) AS browser_lower
    FROM reconciled
    WHERE device = 'desktop'
),
enriched AS (
    SELECT
        e.*,
        d.credit_limit,
        d.city
    FROM base e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
joined AS (
    SELECT
        src.*,
        c.city AS customer_city,
        c.credit_limit AS customer_credit_limit,
        c.name
    FROM enriched src
    INNER JOIN customers c
        ON src.customer_id = c.id
)
SELECT
    customer_id,
    device,
    browser,
    started_at,
    city,
    customer_city,
    customer_credit_limit,
    name,
    CASE WHEN device RLIKE '^(desktop|mobile)$' THEN 'GROUP_B' WHEN device RLIKE '^(tablet)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS device_zone
FROM joined
ORDER BY device DESC NULLS FIRST;
