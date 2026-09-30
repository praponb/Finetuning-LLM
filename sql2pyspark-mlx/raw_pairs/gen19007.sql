WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATEDIFF(CURRENT_DATE(), started_at) / 365 AS years_since_started_at,
        CONCAT_WS('-', browser, country) AS browser_country_key
    FROM sessions
    WHERE started_at > DATE '2023-04-10'
      AND customer_id IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec = 442
        )
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.event_time
    FROM kept s
    JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(years_since_started_at) OVER w_ordered AS next_years_since_started_at,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY customer_id, device ORDER BY years_since_started_at ASC, id ASC)
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    browser,
    country,
    years_since_started_at,
    browser_country_key,
    event_time,
    next_years_since_started_at,
    row_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC) AS seq_no,
    ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY TO_DATE(started_at), device), 4) AS row_seq_share,
    SUM(years_since_started_at) OVER (PARTITION BY MONTH(event_time), browser ORDER BY next_years_since_started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_years_since_started_at
FROM with_windows;
