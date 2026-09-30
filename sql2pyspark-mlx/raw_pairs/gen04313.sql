WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(browser, '/', device) AS browser_device,
        COALESCE(browser, 'DIRECT') AS browser_clean
    FROM sessions
    WHERE country != 'US'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT browser, id
    FROM base
    INTERSECT
    SELECT browser, id
    FROM sessions
    WHERE browser IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.browser = k.browser AND s.id = k.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.country,
        a.browser_device,
        a.browser_clean,
        d.city
    FROM verified a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
    WHERE LOWER(TRIM(d.name)) = 'bob smith'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.name,
        p.country AS customer_country
    FROM with_dims e
    INNER JOIN customers p
        ON e.customer_id = p.id
)
-- Grouped metrics
SELECT
    customer_id,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM enriched
GROUP BY 1
ORDER BY customer_id DESC
