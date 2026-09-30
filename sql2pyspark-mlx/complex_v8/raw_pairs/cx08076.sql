WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        CASE WHEN bpm >= 155 THEN 'A' WHEN bpm >= 104 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM tracks
    WHERE bpm < 165
      AND release_date < DATE '2025-08-02'
),
combined_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        artist_debut_year - LAG(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id) AS artist_debut_year_change,
        LEAD(artist_debut_year, 2) OVER (ORDER BY artist_debut_year DESC, id) AS next2_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS artist_debut_year_avg_2,
        MAX(artist_debut_year) OVER w_artist_id_artist_country AS artist_id_artist_country_max_artist_debut_year,
        DENSE_RANK() OVER (ORDER BY artist_debut_year DESC, id) AS artist_debut_year_dense_rank,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_id), 2) AS artist_debut_year_vs_artist_id_avg,
        MAX(artist_debut_year) OVER (PARTITION BY artist_id) AS artist_id_max_artist_debut_year,
        SUM(artist_debut_year) OVER (PARTITION BY artist_id) AS artist_id_sum_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id, artist_country), 2) AS artist_id_artist_country_avg_artist_debut_year,
        COUNT(*) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(artist_debut_year) OVER w_artist_id, 2) AS artist_id_avg_artist_debut_year,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY artist_id, artist_country), 0), 4) AS artist_debut_year_share_of_artist_id_artist_coun
    FROM combined_details
    WINDOW w_artist_id_artist_country AS (PARTITION BY artist_id, artist_country), w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    artist_debut_year,
    artist_country,
    artist_debut_year_change,
    next2_artist_debut_year,
    artist_debut_year_avg_2,
    artist_debut_year_dense_rank,
    rows_in_artist_id,
    artist_debut_year_vs_artist_id_avg,
    artist_id_max_artist_debut_year,
    running_count,
    artist_id_avg_artist_debut_year,
    artist_debut_year_share_of_artist_id_artist_coun
FROM with_trends
ORDER BY title DESC, running_count DESC
