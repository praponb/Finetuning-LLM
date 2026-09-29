WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        duration_sec + bpm AS duration_sec_plus_bpm,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE bpm <= 155
      AND LENGTH(TRIM(title)) >= 6
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE duration_sec_plus_bpm > (SELECT AVG(duration_sec_plus_bpm) FROM prepared)
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        duration_sec_plus_bpm,
        title_ref,
        title_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.duration_sec_plus_bpm,
        s.title_ref,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM deduped s
    JOIN plays p ON p.track_id = s.id
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM exploded_detail_plays s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.play_listener_id
              AND l.plan = 'family'
              AND SIZE(l.interests) >= 2
        )
),
enriched_play AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.play_id,
        s.play_listener_id,
        s.play_played_at,
        s.play_is_skipped,
        p.listener_id AS play_listener_id_2
    FROM without_matches s
    LEFT JOIN plays p ON p.id = s.play_id
)
SELECT
    play_listener_id_2,
    artist_id,
    GROUPING(play_listener_id_2) AS is_total_play_listener_id_2,
    COUNT(*) AS cnt,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
FROM enriched_play
GROUP BY ROLLUP(play_listener_id_2, artist_id)
ORDER BY play_listener_id_2 NULLS LAST, artist_id NULLS LAST
