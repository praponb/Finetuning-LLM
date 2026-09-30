WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(device)) device_title
    FROM sessions
    WHERE device = 'desktop'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        started_at,
        country,
        device_title,
        latest_rank,
        CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END is_firefox,
        CAST(latest_rank AS DOUBLE) latest_rank_str
    FROM ranked
    WHERE browser IS NOT NULL
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device_title,
        s.latest_rank_str,
        top1.web_event_event_type,
        top1.web_event_duration_sec
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (2, 32)
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
              AND c.session_id IN (1, 4, 22, 29)
        )
)
-- Aggregate with conditional metrics
SELECT
    web_event_event_type,
    MAX(latest_rank_str) AS max_latest_rank_str,
    SORT_ARRAY(COLLECT_SET(web_event_event_type)) AS web_event_event_type_list,
    ROUND(SUM(latest_rank_str) / NULLIF(COUNT(CASE WHEN web_event_event_type = 'search' THEN 1 END), 0), 2) AS latest_rank_str_per_search,
    APPROX_PERCENTILE(web_event_duration_sec, 0.5) AS web_event_duration_sec_p50
FROM kept
GROUP BY CUBE (web_event_event_type)
HAVING latest_rank_str_per_search < 1000000
