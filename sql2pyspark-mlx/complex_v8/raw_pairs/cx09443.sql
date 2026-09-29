WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        CASE WHEN bpm >= 142 THEN 'large' WHEN bpm >= 100 THEN 'medium' ELSE 'small' END AS bpm_tier,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE SIZE(moods) >= 2
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm_tier,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec ASC NULLS LAST) AS duration_sec_rank,
        COUNT(*) OVER w_bpm_tier_artist_id AS rows_in_bpm_tier_artist_id,
        MIN(artist_debut_year) OVER w_bpm_tier_artist_id AS bpm_tier_artist_id_min_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY bpm_tier, artist_id ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_2_prior,
        COUNT(*) OVER (PARTITION BY artist_genre) AS rows_in_artist_genre,
        SUM(duration_sec) OVER w_artist_genre AS artist_genre_sum_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_genre), 2) AS duration_sec_vs_artist_genre_avg
    FROM with_parent
    WINDOW w_bpm_tier_artist_id AS (PARTITION BY bpm_tier, artist_id), w_artist_genre AS (PARTITION BY artist_genre)
),
grouped AS (
    -- Group-level totals
    SELECT
        artist_is_verified,
        artist_id,
        bpm_tier,
        COUNT(DISTINCT artist_genre) AS distinct_artist_genres,
        PERCENTILE_APPROX(artist_debut_year, 0.5) AS median_artist_debut_year,
        ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list,
        COUNT(*) FILTER (WHERE artist_is_verified = TRUE) AS n_artist_is_verified_true
    FROM with_trends
    GROUP BY artist_is_verified, artist_id, bpm_tier
    HAVING AVG(bpm_tier_artist_id_min_artist_debut_year) > 5638
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        artist_is_verified,
        artist_id,
        bpm_tier,
        median_artist_debut_year,
        artist_genre_list,
        SIZE(COLLECT_SET(bpm_tier) OVER (PARTITION BY n_artist_is_verified_true, artist_is_verified)) AS distinct_bpm_tier_in_n_artist_is_verified_true_a,
        LAG(median_artist_debut_year, 2) OVER (PARTITION BY bpm_tier ORDER BY distinct_artist_genres DESC, artist_is_verified, artist_id, bpm_tier) AS prev2_median_artist_debut_year,
        ROUND(median_artist_debut_year - AVG(median_artist_debut_year) OVER (PARTITION BY n_artist_is_verified_true, artist_is_verified), 2) AS median_artist_debut_year_vs_n_artist_is_verified
    FROM grouped
)
SELECT
    artist_is_verified,
    artist_genre_list,
    prev2_median_artist_debut_year,
    median_artist_debut_year_vs_n_artist_is_verified
FROM ranked
ORDER BY prev2_median_artist_debut_year NULLS LAST
