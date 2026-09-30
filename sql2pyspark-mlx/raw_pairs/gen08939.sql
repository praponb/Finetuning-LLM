WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('year', started_at) started_at_period,
        SUBSTRING(browser, 1, 3) browser_prefix,
        COALESCE(browser, 'N/A') resolved_browser
    FROM sessions
    WHERE country IN ('US', 'TH', 'DE')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        browser_prefix
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM deduped
    EXCEPT ALL
    SELECT customer_id
    FROM sessions
    WHERE customer_id IN (1, 7, 15, 16)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.browser,
        a.started_at,
        d.city,
        d.name,
        d.signup_date
    FROM reconciled a
    LEFT JOIN customers d
        ON a.customer_id = d.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    MIN(started_at) AS started_at,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    MIN(started_at) AS started_at_2,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows
FROM enriched
GROUP BY DATE_TRUNC('WEEK', started_at)
ORDER BY cnt DESC;
