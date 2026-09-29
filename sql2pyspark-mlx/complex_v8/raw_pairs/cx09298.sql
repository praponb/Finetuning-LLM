WITH recent_plays AS (
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
        COALESCE(ms_played, 0) AS ms_played_filled,
        CASE WHEN source IN ('album', 'playlist') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE source <> 'radio'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN ms_played_filled >= 281500 THEN 'HIGH' WHEN ms_played_filled >= 103750 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_filled_tier,
        ms_played_filled + ms_played AS ms_played_filled_plus_ms_played
    FROM recent_plays
),
grouped AS (
    -- Summarise per group
    SELECT
        track_id,
        is_skipped,
        ms_played_filled_tier,
        MIN(played_at) AS first_played_at,
        COUNT(DISTINCT ms_played_filled_tier) AS distinct_ms_played_filled_tiers,
        SUM(CASE WHEN device = 'phone' THEN ms_played_filled_plus_ms_played ELSE 0 END) AS ms_played_filled_plus_ms_played_device_phone,
        MAX(played_at) AS last_played_at
    FROM annotated
    GROUP BY track_id, is_skipped, ms_played_filled_tier
    HAVING AVG(ms_played_filled_plus_ms_played) > 124250
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.track_id,
        s.is_skipped,
        s.ms_played_filled_tier,
        s.first_played_at,
        s.ms_played_filled_plus_ms_played_device_phone,
        s.last_played_at,
        t.duration_sec AS track_duration_sec,
        t.artist_id AS track_artist_id,
        t.bpm AS track_bpm
    FROM grouped s
    JOIN tracks t ON t.id = s.track_id
),
latest_per_key AS (
    -- Keep the latest row per track_artist_id
    SELECT
        track_id,
        is_skipped,
        ms_played_filled_tier,
        first_played_at,
        ms_played_filled_plus_ms_played_device_phone,
        last_played_at,
        track_duration_sec,
        track_artist_id,
        track_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_artist_id ORDER BY track_duration_sec DESC, track_id DESC, is_skipped DESC, ms_played_filled_tier DESC) AS rn
        FROM enriched_track
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY MONTH(last_played_at)), 0), 4) AS track_duration_sec_share_of_last_played_at_month,
        RANK() OVER (PARTITION BY is_skipped ORDER BY first_played_at DESC) AS first_played_at_rank,
        FIRST_VALUE(track_duration_sec) OVER (PARTITION BY ms_played_filled_tier ORDER BY ms_played_filled_plus_ms_played_device_phone DESC, track_id, is_skipped, ms_played_filled_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_duration_sec,
        NTILE(5) OVER (PARTITION BY is_skipped ORDER BY first_played_at DESC, track_id, is_skipped, ms_played_filled_tier) AS first_played_at_quintile,
        LAST_VALUE(last_played_at) IGNORE NULLS OVER (PARTITION BY ms_played_filled_tier ORDER BY ms_played_filled_plus_ms_played_device_phone DESC, track_id, is_skipped, ms_played_filled_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_played_at
    FROM latest_per_key
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.track_artist_id
              AND t.duration_sec < 312
        )
      AND s.first_track_duration_sec >= 209
)
SELECT
    track_id,
    is_skipped,
    first_played_at,
    last_played_at,
    track_duration_sec,
    track_artist_id,
    track_bpm,
    track_duration_sec_share_of_last_played_at_month,
    first_played_at_rank,
    first_track_duration_sec,
    last_known_last_played_at
FROM screened
ORDER BY first_played_at_rank DESC NULLS FIRST, track_artist_id DESC
