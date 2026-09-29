WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = FALSE
      AND duration_sec <= 352
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        artist_country,
        artist_is_verified,
        DENSE_RANK() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year DESC) AS artist_debut_year_dense_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        NTILE(4) OVER (ORDER BY duration_sec ASC, id) AS duration_sec_quartile,
        LAST_VALUE(artist_debut_year) OVER (ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        NTILE(5) OVER (PARTITION BY artist_id ORDER BY release_date, id) AS release_date_quintile,
        ROUND(AVG(artist_debut_year) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS artist_debut_year_avg_4,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_country, artist_is_verified) AS rows_in_artist_country_artist_is_verified,
        RANK() OVER (ORDER BY duration_sec ASC, id) AS duration_sec_rank,
        NTILE(10) OVER w_artist_country_is_explicit_b AS release_date_decile,
        LEAD(duration_sec) OVER (PARTITION BY artist_country, is_explicit ORDER BY release_date, id) AS next_duration_sec
    FROM with_parent_artist
    WINDOW w_artist_country_is_explicit_b AS (PARTITION BY artist_country, is_explicit ORDER BY release_date, id), w_artist_country_artist_is_ver AS (PARTITION BY artist_country, artist_is_verified)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        title,
        release_date,
        artist_country,
        artist_is_verified,
        duration_sec_avg_6_prior,
        second_artist_debut_year,
        duration_sec_quartile,
        last_artist_debut_year,
        release_date_quintile,
        artist_debut_year_avg_4,
        second_duration_sec,
        rows_in_artist_country_artist_is_verified,
        duration_sec_rank,
        release_date_decile,
        next_duration_sec
    FROM with_windows
    WHERE artist_debut_year_dense_rank <= 3
      AND release_date < DATE '2025-03-29'
)
SELECT
    id,
    artist_id,
    release_date,
    artist_country,
    artist_is_verified,
    duration_sec_avg_6_prior,
    second_artist_debut_year,
    release_date_quintile,
    second_duration_sec,
    rows_in_artist_country_artist_is_verified,
    duration_sec_rank,
    release_date_decile
FROM top_per_group
ORDER BY second_duration_sec
