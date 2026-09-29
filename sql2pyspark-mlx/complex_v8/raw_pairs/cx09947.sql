WITH scoped_plays AS (
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
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 11:00:00'
      AND listener_id <= 10
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device)) AS distinct_track_id_in_device,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(CUME_DIST() OVER (ORDER BY ms_played_filled ASC, id), 3) AS ms_played_filled_cume_dist,
        NTILE(4) OVER w_all_by_ms_played_filled AS ms_played_filled_quartile
    FROM scoped_plays
    WINDOW w_all_by_ms_played_filled AS (ORDER BY ms_played_filled ASC, id)
),
qualified AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND UPPER(TRIM(l.username)) LIKE 'H%'
              AND YEAR(l.signup_date) = 2024
        )
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_ref,
        s.ms_played_filled,
        s.ms_played_filled_cume_dist,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM qualified s
    JOIN plays p ON p.listener_id = s.listener_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT source, play_played_at, device, ms_played
    FROM line_level_plays
) src
PIVOT (
    MIN(ms_played)
    FOR device IN ('tv', 'speaker', 'car')
)
