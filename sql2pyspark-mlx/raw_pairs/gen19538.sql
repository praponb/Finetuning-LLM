WITH latest AS (
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) = 1
),
kept AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
),
kept_sessions AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
),
latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        latest.page
    FROM kept_sessions s
    LEFT JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
remaining_keys AS (
    SELECT id, customer_id
    FROM latest_match
    EXCEPT ALL
    SELECT id, customer_id
    FROM sessions
    WHERE device = 'tablet'
),
kept_rows AS (
    SELECT s.*
    FROM latest_match s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
cleaned AS (
    SELECT
        *,
        CASE WHEN page = '/checkout' THEN 1 ELSE 0 END AS is_checkout,
        COALESCE(page, 'Unknown') AS page_filled
    FROM kept_rows
    WHERE page != '/checkout'
)
SELECT
    id,
    customer_id,
    page,
    is_checkout,
    page_filled
FROM cleaned
ORDER BY is_checkout DESC;
