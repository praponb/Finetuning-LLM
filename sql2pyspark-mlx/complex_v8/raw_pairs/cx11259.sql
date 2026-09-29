WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm
    FROM tracks
    WHERE duration_sec < 312
),
with_parent AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(bpm) OVER (PARTITION BY artist_id, artist_country) AS artist_id_artist_country_max_bpm,
        artist_debut_year - LAG(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id) AS artist_debut_year_change,
        NTILE(4) OVER (PARTITION BY artist_country ORDER BY artist_debut_year DESC, id) AS artist_debut_year_quartile,
        artist_debut_year - LAG(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY bpm DESC NULLS FIRST, id) AS artist_debut_year_change_2,
        ROUND(AVG(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_2_prior,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id, artist_country ORDER BY artist_debut_year DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_2_prior_2,
        NTILE(10) OVER (ORDER BY artist_debut_year DESC NULLS FIRST, id) AS artist_debut_year_decile,
        MAX(bpm) OVER w_artist_id AS artist_id_max_bpm,
        MIN(bpm) OVER (PARTITION BY artist_id, artist_country ORDER BY artist_debut_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, artist_country ORDER BY artist_debut_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm
    FROM with_parent
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(artist_debut_year_change / 1000) * 1000 AS artist_debut_year_change_bucket,
        COALESCE(artist_country, 'UNKNOWN') AS artist_country_or_unknown,
        ABS(artist_debut_year_change - -809) AS artist_debut_year_change_distance
    FROM windowed
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    artist_debut_year,
    artist_id_artist_country_max_bpm,
    artist_debut_year_change,
    artist_debut_year_quartile,
    artist_debut_year_avg_2_prior,
    artist_debut_year_avg_2_prior_2,
    artist_debut_year_decile,
    artist_id_max_bpm,
    second_bpm,
    artist_debut_year_change_bucket,
    artist_debut_year_change_distance
FROM enriched
ORDER BY title
