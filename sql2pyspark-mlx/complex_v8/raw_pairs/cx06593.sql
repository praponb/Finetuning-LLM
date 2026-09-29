WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
      AND source NOT IN ('album', 'playlist')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        RANK() OVER (ORDER BY played_at) AS played_at_rank,
        ROW_NUMBER() OVER (ORDER BY played_at, id) AS rn
    FROM valid_plays
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE rn = 1
),
unique_rows AS (
    -- Keep the latest row per source
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        played_at_rank,
        rn
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY rn DESC, id DESC) AS rn_2
        FROM top_rows
    ) ranked
    WHERE rn_2 = 1
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.rn,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM unique_rows s
    JOIN plays p ON p.listener_id = s.listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM line_level_plays s
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM summarized_rows
    WHERE ms_played > (SELECT AVG(ms_played) FROM summarized_rows)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    rn,
    play_id,
    play_ms_played,
    play_played_at,
    play_count,
    last_played_at,
    first_played_at,
    ROUND((UNIX_TIMESTAMP(last_played_at) - UNIX_TIMESTAMP(first_played_at)) / 60, 1) AS minutes_first_played_at_to_last_played_at
FROM screened
WHERE play_ms_played > 129336
ORDER BY rn, minutes_first_played_at_to_last_played_at DESC
