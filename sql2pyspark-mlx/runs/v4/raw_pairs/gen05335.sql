WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS event_seq
    FROM sessions
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
),
eligible_sessions AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND TO_DATE(x.event_time) < '2025-10-04'
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        seq_no,
        row_seq,
        event_seq,
        LEAST(seq_no, row_seq) AS least_seq_no_row_seq,
        HOUR(started_at) AS started_at_hour,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM eligible_sessions
    WHERE started_at >= DATE '2023-03-19'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.age,
        d.email,
        d.credit_limit
    FROM base e
    LEFT JOIN customers d
        ON e.customer_id = d.id
)
-- Grouped metrics
SELECT
    browser,
    device,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT_IF(country = 'US') AS us_events,
    PERCENT_RANK() OVER (PARTITION BY browser ORDER BY COUNT_IF(country = 'US') DESC) AS us_events_percent_rank
FROM joined
GROUP BY browser, device
ORDER BY browser DESC
