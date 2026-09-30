WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        started_at,
        DATE_FORMAT(started_at, 'MMM yyyy') started_at_label,
        COALESCE(country, 'N/A') resolved_country
    FROM sessions
    WHERE started_at >= DATE '2023-12-12'
      AND country IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        started_at,
        started_at_label,
        resolved_country,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) row_seq,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) row_seq_2,
        ROW_NUMBER() OVER (ORDER BY started_at, id) row_seq_3
    FROM kept
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_event_type
    FROM islands s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device, web_event_event_type ORDER BY is_new_burst DESC, id) AS dedup_rank
        FROM latest_match
    ) ranked
    WHERE dedup_rank = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device, web_event_event_type ORDER BY row_seq_3 NULLS FIRST, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        island_id,
        device,
        DATE_TRUNC('YEAR', started_at) AS year_start,
        COUNT(*) FILTER (WHERE web_event_event_type = 'click') AS click_rows,
        COUNT(*) AS cnt,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
        COUNT(DISTINCT device) AS distinct_devices,
        SUM(CASE WHEN device = 'tablet' THEN row_seq ELSE 0 END) AS tablet_row_seq,
        SUM(CASE WHEN device = 'mobile' THEN row_seq_3 ELSE 0 END) AS mobile_row_seq_3
    FROM latest
    GROUP BY CUBE (island_id, device, DATE_TRUNC('YEAR', started_at))
)
SELECT
    device,
    click_rows,
    cnt,
    tablet_row_seq,
    mobile_row_seq_3
FROM totals
ORDER BY cnt
