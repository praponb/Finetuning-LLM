WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        INITCAP(TRIM(device)) AS device_title,
        COALESCE(country, 'NONE') AS country_filled
    FROM sessions
    WHERE started_at > DATE '2023-04-17'
      AND browser IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        started_at,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
    FROM base
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label,
        CASE WHEN device IN ('tablet', 'desktop') THEN 'Priority' ELSE 'Other' END AS device_group
    FROM metrics
    WHERE started_at >= '2024-02-01'
),
cleaned_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        first_known_browser,
        started_at_label,
        device_group,
        TRIM(device) AS clean_device,
        LOWER(device) AS device_lower
    FROM cleaned
    WHERE customer_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM cleaned_sessions
    EXCEPT ALL
    SELECT customer_id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2024-08-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned_sessions s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.started_at_label,
        a.clean_device,
        b.city,
        b.country
    FROM reconciled a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.credit_limit,
        b.age
    FROM joined src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            city,
            country,
            credit_limit
        FROM enriched
    ) src
    PIVOT (
        AVG(credit_limit) AS total
        FOR country IN ('JP', 'UK', 'SG')
    )
)
SELECT city
FROM wide
ORDER BY city DESC NULLS FIRST
LIMIT 10
