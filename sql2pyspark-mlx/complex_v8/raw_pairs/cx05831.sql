WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE bpm < 143
      AND is_explicit = TRUE
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY title_len) AS title_len_sum_duration_sec,
        FIRST_VALUE(bpm) OVER (PARTITION BY title_len ORDER BY title_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        MIN(duration_sec) OVER (PARTITION BY title_len) AS title_len_min_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_6_prior,
        ROUND(CUME_DIST() OVER w_title_len_artist_id_by_durat, 3) AS duration_sec_cume_dist
    FROM recent_tracks
    WINDOW w_title_len_artist_id_by_durat AS (PARTITION BY title_len, artist_id ORDER BY duration_sec NULLS LAST)
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.title_len,
        s.title_len_sum_duration_sec,
        s.title_len_min_duration_sec,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM metrics s
    JOIN plays p ON p.track_id = s.id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(play_ms_played) OVER (PARTITION BY artist_id, play_id ORDER BY title_len DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_play_ms_played,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY title_len ORDER BY title_len ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(AVG(title_len) OVER (PARTITION BY artist_id, play_id ORDER BY title_len DESC, id, play_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS title_len_avg_7
    FROM with_children
)
SELECT *
FROM (
    SELECT release_date, artist_id, play_source, title_len
    FROM with_trends
) src
PIVOT (
    SUM(title_len)
    FOR play_source IN ('album', 'playlist', 'radio')
)
ORDER BY release_date, artist_id
