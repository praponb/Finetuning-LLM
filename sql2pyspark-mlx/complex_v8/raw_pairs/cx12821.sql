WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE is_skipped = TRUE
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device
    FROM base_plays s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device <> 'speaker'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(PERCENT_RANK() OVER w_is_skipped_by_played_at_ties, 3) AS played_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY play_id, play_source ORDER BY ms_played ASC), 3) AS ms_played_percent_rank
    FROM detail_rows
    WINDOW w_is_skipped_by_played_at_ties AS (PARTITION BY is_skipped ORDER BY played_at ASC)
),
joined_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.play_id,
        s.play_device,
        s.second_ms_played,
        s.played_at_percent_rank,
        s.ms_played_percent_rank,
        t.bpm AS track_bpm
    FROM metrics s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT *
FROM (
    SELECT track_id, played_at, device, played_at_percent_rank
    FROM joined_track
) src
PIVOT (
    MAX(played_at_percent_rank)
    FOR device IN ('speaker', 'desktop')
)
