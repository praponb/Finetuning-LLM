WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 301865 THEN 'large' WHEN ms_played >= 71476 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 15:00:00'
      AND device = 'speaker'
),
grouped AS (
    -- Summarise per group
    SELECT
        track_id,
        source_len,
        ROUND(SUM(source_len) / NULLIF(SUM(ms_played), 0), 3) AS source_len_to_ms_played_ratio,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        SUM(CASE WHEN NOT is_skipped THEN 1 ELSE 0 END) AS n_not_is_skipped
    FROM filtered_plays
    WHERE source_len >= 4
    GROUP BY track_id, source_len
    HAVING AVG(ms_played) > 56858
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.track_id) AS avg_play_ms_played
    FROM grouped s
)
-- Final projection
SELECT
    track_id,
    source_len_to_ms_played_ratio,
    device_list,
    source_list,
    n_not_is_skipped
FROM with_counts
WHERE source_len_to_ms_played_ratio < 6.0
