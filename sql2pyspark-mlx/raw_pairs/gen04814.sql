WITH with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.country,
        top1.web_event_page,
        top1.web_event_event_type
    FROM sessions s
    JOIN LATERAL (
            SELECT c.page AS web_event_page, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.web_event_page,
        latest.duration_sec,
        latest.page
    FROM with_last s
    JOIN LATERAL (
            SELECT c.duration_sec, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_sessions AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.page,
        x.event_type,
        x.web_event_duration_sec
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page IS NOT NULL
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        page,
        event_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY web_event_duration_sec DESC NULLS LAST, id ASC) AS rank_in_group
        FROM kept
    ) ranked
    WHERE rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM deduped
    EXCEPT ALL
    SELECT id
    FROM sessions
    WHERE country = 'JP'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id = 22
        )
)
-- Grouped metrics
SELECT
    event_type,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows
FROM eligible
GROUP BY 1
ORDER BY search_rows DESC NULLS LAST
