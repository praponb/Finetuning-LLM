WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE track_id >= 24
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM valid_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device <> 'phone'
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        NTILE(10) OVER (ORDER BY played_at, id) AS played_at_decile,
        SUM(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3
    FROM without_matches
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN ms_played >= 318978 THEN 'A' WHEN ms_played >= 92277 THEN 'B' ELSE 'C' END AS ms_played_tier,
        CASE WHEN ms_played >= 245877 THEN 'A' WHEN ms_played >= 151112 THEN 'B' ELSE 'C' END AS ms_played_tier_2
    FROM windowed
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.ms_played_vs_listener_id_avg,
        s.played_at_decile,
        s.ms_played_tier,
        s.ms_played_tier_2,
        l.username AS listener_username,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit
    FROM scored s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        source,
        ms_played_vs_listener_id_avg,
        ms_played_tier,
        ms_played_tier_2,
        track_bpm,
        ROUND(AVG(played_at_decile) OVER (ORDER BY ms_played_vs_listener_id_avg DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS played_at_decile_avg_3_prior,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY ms_played_tier_2), 0), 4) AS track_bpm_share_of_ms_played_tier_2,
        MIN(ms_played) OVER (PARTITION BY ms_played_tier_2) AS ms_played_tier_2_min_ms_played,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY track_bpm DESC, id) AS track_bpm_seq,
        MAX(played_at_decile) OVER (PARTITION BY ms_played_tier_2) AS ms_played_tier_2_max_played_at_decile
    FROM with_details
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) FILTER (WHERE source <> 'playlist') AS play_source_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM ranked s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source,
    ms_played_vs_listener_id_avg,
    track_bpm,
    played_at_decile_avg_3_prior,
    track_bpm_share_of_ms_played_tier_2,
    ms_played_tier_2_min_ms_played,
    ms_played_tier_2_max_played_at_decile
FROM summarized_rows
ORDER BY track_bpm
