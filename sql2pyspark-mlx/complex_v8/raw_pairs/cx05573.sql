WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        UPPER(TRIM(device)) AS device_clean,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        YEAR(played_at) AS played_at_year
    FROM plays
    WHERE HOUR(played_at) BETWEEN 9 AND 15
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        device_clean,
        device_prefix,
        played_at_year,
        MAX(played_at_year) OVER (PARTITION BY is_skipped, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_played_at_year_so_far,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, source ORDER BY played_at, id), 3) AS played_at_cume_dist,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY is_skipped, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        COUNT(*) OVER (PARTITION BY is_skipped, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(played_at_year) IGNORE NULLS OVER (PARTITION BY is_skipped, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_year
    FROM scoped
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.device_clean,
        s.max_played_at_year_so_far,
        s.played_at_cume_dist,
        s.last_known_played_at,
        s.running_count,
        s.first_known_played_at_year,
        t.bpm AS track_bpm
    FROM with_windows s
    JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    played_at_cume_dist,
    last_known_played_at,
    running_count,
    first_known_played_at_year,
    track_bpm,
    track_bpm - played_at_cume_dist AS track_bpm_minus_played_at_cume_dist
FROM with_details_track
WHERE running_count > 3
ORDER BY track_bpm_minus_played_at_cume_dist NULLS LAST, id
LIMIT 5
