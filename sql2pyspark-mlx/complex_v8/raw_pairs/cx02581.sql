WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE listener_id >= 4
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        source,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg,
        COUNT(*) OVER (ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_all_by_ms_played_ties AS ms_played_dense_rank,
        MAX(ms_played) OVER w_source AS source_max_ms_played
    FROM prepared_plays
    WINDOW w_all_by_ms_played_ties AS (ORDER BY ms_played DESC NULLS FIRST), w_source AS (PARTITION BY source)
),
enriched AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.source_max_ms_played,
        l.birth_year AS listener_birth_year,
        t.moods AS track_moods
    FROM metrics s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE l.birth_year BETWEEN 1966 AND 1981
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.source_max_ms_played,
        s.track_moods,
        t.moods AS track_moods_2,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec
    FROM enriched s
    JOIN tracks t ON t.id = s.track_id
),
latest_per_key AS (
    -- Keep the latest row per source, track_id
    SELECT
        id,
        listener_id,
        track_id,
        source,
        source_max_ms_played,
        track_moods,
        track_moods_2,
        track_artist_id,
        track_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY track_duration_sec DESC, id DESC) AS recency_rank
        FROM combined_details_track
    ) ranked
    WHERE recency_rank = 1
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM latest_per_key s
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    source,
    track_moods,
    track_moods_2,
    track_artist_id,
    track_duration_sec
FROM profiled
WHERE play_count >= 2
ORDER BY listener_id
