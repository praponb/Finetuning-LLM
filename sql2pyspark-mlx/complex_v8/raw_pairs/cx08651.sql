WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE listener_id <= 18
      AND device = 'speaker'
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM base_plays s
),
with_parent_track AS (
    -- Join tracks
    SELECT s.*, t.duration_sec AS track_duration_sec
    FROM with_counts s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(min_play_ms_played) OVER (PARTITION BY device, listener_id ORDER BY max_play_ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_min_play_ms_played,
        FIRST_VALUE(max_play_ms_played) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY track_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_play_ms_played
    FROM with_parent_track
),
stats AS (
    -- Summarise per group
    SELECT
        source,
        play_count,
        DATE_FORMAT(played_at_date, 'yyyy-MM') AS played_at_date_ym,
        ROUND(AVG(first_known_max_play_ms_played), 2) AS avg_first_known_max_play_ms_played,
        COUNT(*) AS n_rows,
        DATEDIFF(MAX(played_at_date), MIN(played_at_date)) AS played_at_date_span_days,
        SUM(CASE WHEN source = 'search' THEN first_known_max_play_ms_played ELSE 0 END) AS first_known_max_play_ms_played_source_search
    FROM with_trends
    GROUP BY source, play_count, DATE_FORMAT(played_at_date, 'yyyy-MM')
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE n_rows > (SELECT MIN(n_rows) FROM stats)
)
SELECT source, play_count, measure, amount
FROM (
    SELECT source, play_count, first_known_max_play_ms_played_source_search, played_at_date_span_days, n_rows
    FROM qualified
) src
UNPIVOT (
    amount FOR measure IN (first_known_max_play_ms_played_source_search, played_at_date_span_days, n_rows)
) unp
ORDER BY source, play_count, measure
