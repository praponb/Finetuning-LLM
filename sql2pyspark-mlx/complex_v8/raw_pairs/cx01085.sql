WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 312) AS duration_sec_distance
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'S%' OR is_explicit = TRUE)
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.duration_sec_distance,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM filtered s
    JOIN plays p ON p.track_id = s.id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        play_id,
        play_is_skipped,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, play_is_skipped), 0), 4) AS duration_sec_share_of_artist_id_play_is_skipped,
        LAST_VALUE(bpm) OVER (ORDER BY duration_sec DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        LAST_VALUE(play_played_at) IGNORE NULLS OVER (ORDER BY duration_sec_distance ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_played_at,
        ROW_NUMBER() OVER w_all_by_duration_sec_distance_2 AS seq_no
    FROM exploded_detail
    WINDOW w_all_by_duration_sec_distance_2 AS (ORDER BY duration_sec_distance ASC, id, play_id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        play_id,
        play_is_skipped,
        duration_sec_share_of_artist_id_play_is_skipped,
        last_bpm,
        last_known_play_played_at
    FROM metrics
    WHERE seq_no <= 3
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.play_id,
        s.play_is_skipped,
        s.duration_sec_share_of_artist_id_play_is_skipped,
        s.last_known_play_played_at,
        a.artist_name
    FROM top_rows s
    JOIN artists a ON a.id = s.artist_id
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT(artist_name, '#', CAST(play_id AS STRING)) AS artist_name_ref
    FROM with_parent
    WHERE UPPER(TRIM(artist_name)) LIKE 'S%'
)
-- Final projection
SELECT
    id,
    play_id,
    play_is_skipped,
    duration_sec_share_of_artist_id_play_is_skipped,
    last_known_play_played_at,
    artist_name,
    artist_name_ref,
    SUBSTRING(TRIM(artist_name_ref), 1, 4) AS artist_name_ref_prefix
FROM annotated
ORDER BY id DESC
