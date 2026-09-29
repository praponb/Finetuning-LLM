WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER w_source_track_id) AS distinct_listener_id_in_source_track_id,
        LEAD(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS next_ms_played,
        LEAD(ms_played) OVER (PARTITION BY listener_id, source ORDER BY ms_played, id) AS next_ms_played_2,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY source, track_id), 0), 4) AS ms_played_share_of_source_track_id,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id), 0), 4) AS ms_played_share_of_track_id,
        SUM(ms_played) OVER (PARTITION BY listener_id, source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id, source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        MIN(ms_played) OVER w_track_id AS track_id_min_ms_played,
        SUM(ms_played) OVER (PARTITION BY source ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_sum_2_prior,
        NTILE(3) OVER (PARTITION BY listener_id, source ORDER BY ms_played, id) AS ms_played_tercile
    FROM filtered_plays
    WINDOW w_source_track_id AS (PARTITION BY source, track_id), w_track_id AS (PARTITION BY track_id)
),
totals AS (
    -- Group-level totals
    SELECT
        is_skipped,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        ROUND(100 * COUNT(CASE WHEN source IN ('radio', 'album', 'search') THEN 1 END) / COUNT(*), 1) AS pct_source_in_radio_album_search,
        MIN(track_id_min_ms_played) AS min_track_id_min_ms_played
    FROM with_windows
    GROUP BY 1
)
-- Final projection
SELECT
    is_skipped,
    distinct_listener_ids,
    pct_source_in_radio_album_search,
    min_track_id_min_ms_played,
    ABS(distinct_listener_ids - 7) AS distinct_listener_ids_distance
FROM totals
WHERE min_track_id_min_ms_played <= 2601750
