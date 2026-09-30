WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE is_explicit = FALSE
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        a.genre AS artist_genre
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'Pop'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        SUM(bpm) OVER w_artist_genre AS artist_genre_sum_bpm,
        LAG(bpm, 1, 0) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS prev_bpm,
        ROW_NUMBER() OVER w_artist_genre_artist_id_by_bp AS bpm_seq,
        LEAD(bpm, 3, 0) OVER w_artist_id_by_bpm AS next3_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_genre, artist_id), 2) AS bpm_vs_artist_genre_artist_id_avg,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_genre), 2) AS bpm_vs_artist_genre_avg,
        MIN(bpm) OVER w_artist_genre_artist_id AS artist_genre_artist_id_min_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_genre, artist_id), 0), 4) AS bpm_share_of_artist_genre_artist_id,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_genre, artist_id ORDER BY bpm DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior
    FROM joined
    WINDOW w_artist_genre AS (PARTITION BY artist_genre), w_artist_genre_artist_id_by_bp AS (PARTITION BY artist_genre, artist_id ORDER BY bpm NULLS LAST, id), w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm DESC, id), w_artist_genre_artist_id AS (PARTITION BY artist_genre, artist_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        artist_id,
        artist_genre_sum_bpm,
        prev_bpm,
        next3_bpm,
        bpm_vs_artist_genre_artist_id_avg,
        bpm_vs_artist_genre_avg,
        artist_genre_artist_id_min_bpm,
        bpm_share_of_artist_genre_artist_id,
        bpm_avg_3_prior
    FROM metrics
    WHERE bpm_seq <= 5
      AND bpm_vs_artist_genre_avg > -54.0
)
SELECT
    id,
    artist_genre_sum_bpm,
    prev_bpm,
    next3_bpm,
    bpm_vs_artist_genre_artist_id_avg,
    bpm_vs_artist_genre_avg,
    artist_genre_artist_id_min_bpm,
    bpm_avg_3_prior,
    COALESCE(bpm_avg_3_prior, 0.0) AS bpm_avg_3_prior_filled
FROM top_rows
ORDER BY bpm_avg_3_prior_filled DESC, id
