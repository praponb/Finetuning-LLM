WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(country, ', ', device) AS country_device,
        SUBSTRING(country, 1, 4) AS country_prefix
    FROM sessions
    WHERE browser IS NOT NULL
      AND country IN ('SG', 'JP', 'US')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, country
    FROM filtered
    INTERSECT
    SELECT customer_id, country
    FROM sessions
    WHERE device != 'mobile'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.country = k.country
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_device,
        country_prefix,
        CONCAT(browser, ', ', device) AS browser_device,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM verified
    WHERE started_at >= DATE '2023-07-28'
      AND customer_id IS NOT NULL
),
filtered_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(started_at AS DATE) AS started_at_day,
        LENGTH(country) AS country_length
    FROM cleaned
    WHERE country IN ('TH', 'DE')
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.segment,
        d.country AS customer_country
    FROM filtered_sessions a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE d.segment = 'Consumer'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, country
    FROM joined
    INTERSECT
    SELECT customer_id, country
    FROM sessions
    WHERE device = 'mobile'
),
verified_sessions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.country = k.country
)
SELECT
    customer_id,
    device,
    browser,
    started_at,
    country,
    country_device,
    country_prefix,
    browser_device,
    started_at_day
FROM verified_sessions;
