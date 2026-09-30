WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE MONTH(played_at) IN (1, 3)
      AND source IN ('playlist', 'album', 'search')
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM base_plays
),
with_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.source_ref,
        t.title AS track_title
    FROM annotated s
    LEFT JOIN tracks t ON t.id = s.track_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(ms_played) AS max_play_ms_played
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played
    FROM with_details s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        ROUND(AVG(max_play_ms_played), 2) AS avg_max_play_ms_played,
        COUNT(*) AS cnt
    FROM with_counts
    WHERE UPPER(TRIM(track_title)) LIKE 'P%'
    GROUP BY is_skipped
    HAVING COUNT(*) >= 3
)
SELECT
    cnt,
    is_skipped,
    COUNT(*) AS cnt_2,
    ROUND(STDDEV(cnt), 2) AS stddev_cnt
FROM grouped
GROUP BY ROLLUP(cnt, is_skipped)
ORDER BY cnt NULLS LAST, is_skipped NULLS LAST
