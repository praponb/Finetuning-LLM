WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE (bpm >= 76 OR duration_sec BETWEEN 183 AND 392)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.duration_sec_filled,
        s.days_since_release_date,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM valid s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        duration_sec_filled,
        days_since_release_date,
        artist_is_verified,
        MIN(bpm) OVER w_artist_genre_artist_is_verif AS artist_genre_artist_is_verified_min_bpm,
        ROUND(AVG(duration_sec_filled) OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_filled_avg_3_prior,
        ROUND(AVG(days_since_release_date) OVER (PARTITION BY artist_id ORDER BY days_since_release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_release_date_avg_3,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC) AS days_since_release_date_dense_rank,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY artist_genre, artist_is_verified), 0), 4) AS artist_debut_year_share_of_artist_genre_artist_i,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_genre, artist_is_verified), 2) AS artist_genre_artist_is_verified_avg_artist_debut,
        ROW_NUMBER() OVER w_artist_is_verified_by_durati AS duration_sec_filled_seq,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY days_since_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(days_since_release_date / NULLIF(SUM(days_since_release_date) OVER w_artist_genre_artist_is_verif, 0), 4) AS days_since_release_date_share_of_artist_genre_ar,
        SUM(bpm) OVER (PARTITION BY artist_genre, artist_is_verified) AS artist_genre_artist_is_verified_sum_bpm,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY days_since_release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_max_3
    FROM with_parent
    WINDOW w_artist_genre_artist_is_verif AS (PARTITION BY artist_genre, artist_is_verified), w_artist_is_verified_by_durati AS (PARTITION BY artist_is_verified ORDER BY duration_sec_filled ASC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        days_since_release_date_dense_rank,
        duration_sec_filled_seq,
        ROUND(AVG(duration_sec_filled_avg_3_prior), 2) AS avg_duration_sec_filled_avg_3_prior,
        SUM(CASE WHEN artist_is_verified = FALSE THEN 1 ELSE 0 END) AS n_artist_is_verified_false
    FROM sequenced
    GROUP BY is_explicit, days_since_release_date_dense_rank, duration_sec_filled_seq
)
-- Final projection
SELECT
    is_explicit,
    duration_sec_filled_seq,
    n_artist_is_verified_false,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label,
    FLOOR(duration_sec_filled_seq / 2) * 2 AS duration_sec_filled_seq_bucket
FROM stats
ORDER BY duration_sec_filled_seq_bucket
