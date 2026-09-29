WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 261579 THEN 'large' WHEN ms_played >= 73625 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE device NOT IN ('speaker', 'desktop')
      AND track_id <= 26
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ms_played_tier,
        device_slug,
        COUNT(*) OVER (PARTITION BY ms_played_tier) AS rows_in_ms_played_tier,
        SUM(ms_played) OVER (PARTITION BY ms_played_tier) AS ms_played_tier_sum_ms_played,
        SUM(ms_played) OVER (PARTITION BY source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM filtered
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.rows_in_ms_played_tier,
        s.running_ms_played,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM sequenced s
    JOIN tracks t ON t.id = s.track_id
)
SELECT
    is_skipped,
    listener_id,
    track_id,
    COUNT(*) AS row_count,
    ROUND(SUM(rows_in_ms_played_tier) / NULLIF(SUM(running_ms_played), 0), 3) AS rows_in_ms_played_tier_to_running_ms_played_rati,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined_track
GROUP BY GROUPING SETS ((is_skipped, listener_id, track_id), (is_skipped), ())
