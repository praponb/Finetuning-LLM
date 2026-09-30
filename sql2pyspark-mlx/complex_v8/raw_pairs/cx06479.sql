WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE device NOT IN ('speaker', 'phone')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY ms_played, id) AS ms_played_seq,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY device ORDER BY ms_played, id))) / 60, 1) AS minutes_since_prev_played_at
    FROM prepared
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.device,
        s.ms_played_seq,
        s.second_played_at,
        s.minutes_since_prev_played_at,
        t.artist_id AS track_artist_id
    FROM windowed s
    JOIN tracks t ON t.id = s.track_id
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.device,
        s.ms_played_seq,
        s.second_played_at,
        s.minutes_since_prev_played_at,
        s.track_artist_id,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM enriched_track s
    JOIN artists a ON a.id = s.track_artist_id
    WHERE a.country IS NOT NULL
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM enriched_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = TRUE
        )
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    ms_played_seq,
    artist_is_verified,
    GREATEST(ms_played, ms_played_seq) AS greatest_ms_played_ms_played_seq,
    CASE WHEN ms_played >= 299017 THEN 'large' WHEN ms_played >= 168782 THEN 'medium' ELSE 'small' END AS ms_played_tier
FROM filtered
ORDER BY ms_played_tier DESC
