WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile,
        DATEDIFF(CURRENT_DATE(), started_at) / 365.25 AS years_since_started_at
    FROM sessions
    WHERE browser IS NOT NULL
      AND country = 'US'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        browser,
        is_mobile,
        years_since_started_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_mobile DESC NULLS LAST, id) AS latest_rank
        FROM kept
    ) x
    WHERE latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < '2024-06-12'
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM eligible
    EXCEPT DISTINCT
    SELECT id, customer_id
    FROM sessions
    WHERE browser IN ('Safari', 'Edge')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT
    browser,
    is_mobile,
    years_since_started_at
FROM reconciled
ORDER BY browser DESC NULLS FIRST
