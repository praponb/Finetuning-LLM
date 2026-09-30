WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE track_id IS NOT NULL
      AND source NOT IN ('search', 'album')
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        COALESCE(ms_played, 0) AS ms_played_filled,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM cleaned
    WHERE is_skipped = TRUE
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.ms_played_bucket,
        s.source_or_unknown,
        t.is_explicit AS track_is_explicit,
        t.moods AS track_moods
    FROM scored s
    JOIN tracks t ON t.id = s.track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM enriched s
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM summarized_rows
    WHERE max_play_ms_played > (SELECT AVG(max_play_ms_played) FROM summarized_rows)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ms_played,
        source_or_unknown,
        track_is_explicit,
        track_moods,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY TO_DATE(last_played_at), source)) AS distinct_listener_id_in_last_played_at_to_date_s,
        SUM(ms_played) OVER (PARTITION BY TO_DATE(last_played_at), source) AS last_played_at_to_date_source_sum_ms_played,
        SIZE(COLLECT_SET(track_id) OVER w_last_played_at_to_date_sourc) AS distinct_track_id_in_last_played_at_to_date_sour
    FROM screened
    WINDOW w_last_played_at_to_date_sourc AS (PARTITION BY TO_DATE(last_played_at), source)
)
SELECT
    id,
    ms_played,
    source_or_unknown,
    track_is_explicit,
    track_moods,
    last_played_at_to_date_source_sum_ms_played,
    distinct_track_id_in_last_played_at_to_date_sour,
    INITCAP(TRIM(source_or_unknown)) AS source_or_unknown_clean,
    FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
FROM metrics
ORDER BY distinct_track_id_in_last_played_at_to_date_sour DESC, id
LIMIT 5
