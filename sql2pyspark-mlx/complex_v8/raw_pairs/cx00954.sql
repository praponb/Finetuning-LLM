WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE LOWER(title) LIKE '%mi%'
      AND bpm BETWEEN 76 AND 155
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_slug,
        a.genre AS artist_genre
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY is_explicit, artist_genre ORDER BY bpm DESC, id) AS bpm_rank,
        SUM(bpm) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_sum_3_prior,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC, id) AS bpm_change,
        LEAD(duration_sec, 2) OVER (ORDER BY duration_sec, id) AS next2_duration_sec,
        RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec DESC) AS duration_sec_rank,
        ROUND(AVG(bpm) OVER w_artist_genre, 2) AS artist_genre_avg_bpm,
        SUM(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_genre), 0), 4) AS bpm_share_of_artist_genre,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_genre ORDER BY bpm DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_4
    FROM combined_details_artist
    WINDOW w_artist_genre AS (PARTITION BY artist_genre)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    title_slug,
    artist_genre,
    bpm_sum_3_prior,
    duration_sec_rank,
    artist_genre_avg_bpm
FROM with_trends
ORDER BY duration_sec_rank DESC
