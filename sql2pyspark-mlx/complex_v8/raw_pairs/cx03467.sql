WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source IN ('playlist', 'search', 'radio')
      AND device NOT IN ('tv', 'car')
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped_plays
    WHERE ms_played > (SELECT MIN(ms_played) FROM scoped_plays)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM with_activity
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(played_at), track_id) AS rows_in_played_at_year_track_id,
        ROUND(CUME_DIST() OVER w_source_by_ms_played_ties, 3) AS ms_played_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played_filled, id), 3) AS ms_played_filled_percent_rank
    FROM annotated
    WINDOW w_source_by_ms_played_ties AS (PARTITION BY source ORDER BY ms_played)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source,
    device_prefix,
    ms_played_filled,
    rows_in_played_at_year_track_id,
    ms_played_filled_percent_rank,
    FLOOR(ms_played_filled / 50000) * 50000 AS ms_played_filled_bucket
FROM ranked
ORDER BY ms_played_filled
