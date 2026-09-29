WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.age,
        d.segment,
        d.signup_date
    FROM sessions a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE d.country NOT IN ('TH', 'UK', 'DE')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, browser
    FROM with_dims
    INTERSECT
    SELECT id, browser
    FROM sessions
    WHERE country = 'DE'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.id = k.id AND s.browser = k.browser
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec IS NOT NULL
        )
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT e.*, p.country AS customer_country
    FROM not_flagged e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.signup_date
        )
),
not_flagged_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec != 205
        )
),
with_dims_sessions_2 AS (
    -- Attach customers attributes
    SELECT s.*, c.segment AS customer_segment
    FROM not_flagged_sessions s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        AVG(age) AS avg_age,
        COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_rows,
        ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN customer_country = 'SG' THEN 1 END), 0), 2) AS age_per_sg,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
        MAX(age) AS max_age
    FROM with_dims_sessions_2
    GROUP BY 1
    HAVING avg_age >= 0
)
SELECT
    quarter_start,
    avg_age,
    age_per_sg,
    browser_list
FROM summary
ORDER BY quarter_start DESC NULLS FIRST
