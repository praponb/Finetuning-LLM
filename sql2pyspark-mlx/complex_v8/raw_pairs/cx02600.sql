WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE bpm > 82
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.artist_country,
        s.artist_debut_year,
        s.artist_name,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM with_parent_artist s
    LEFT JOIN plays p ON p.track_id = s.id AND p.ms_played < 223828
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.artist_country,
        s.artist_debut_year,
        s.artist_name,
        s.play_id,
        s.play_is_skipped,
        s.play_device,
        s.play_ms_played,
        a.artist_name AS artist_artist_name,
        a.country AS artist_country_2,
        p.is_skipped AS play_is_skipped_2
    FROM exploded_detail_plays s
    LEFT JOIN artists a ON a.id = s.artist_id
    LEFT JOIN plays p ON p.id = s.play_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(play_ms_played) IGNORE NULLS OVER (PARTITION BY play_is_skipped_2, artist_country_2 ORDER BY artist_debut_year, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_ms_played,
        NTILE(5) OVER (PARTITION BY play_is_skipped_2, artist_country_2 ORDER BY artist_debut_year, id, play_id) AS artist_debut_year_quintile,
        SUM(play_ms_played) OVER (PARTITION BY play_id) AS play_id_sum_play_ms_played
    FROM with_parent
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric, reading
FROM (
    SELECT id, play_id, play_ms_played, artist_debut_year, last_known_play_ms_played, artist_debut_year_quintile
    FROM windowed
) src
UNPIVOT (
    reading FOR metric IN (play_ms_played, artist_debut_year, last_known_play_ms_played, artist_debut_year_quintile)
) unp
ORDER BY id, play_id, metric
