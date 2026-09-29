WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        ABS(bpm - 96) AS bpm_distance,
        CASE WHEN bpm >= 163 THEN 'HIGH' WHEN bpm >= 89 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE LOWER(title) LIKE '%ty%'
      AND duration_sec BETWEEN 142 AND 352
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(bpm_distance) OVER (PARTITION BY bpm_tier, artist_id ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_distance,
        MAX(bpm) OVER w_artist_id AS artist_id_max_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id, 2) AS duration_sec_vs_artist_id_avg,
        RANK() OVER (PARTITION BY bpm_tier, artist_id ORDER BY duration_sec NULLS LAST, id) AS duration_sec_rank,
        NTILE(10) OVER (PARTITION BY bpm_tier, artist_id ORDER BY duration_sec NULLS LAST, id) AS duration_sec_decile,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec
    FROM cleaned_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(last_bpm_distance) OVER (ORDER BY bpm_distance, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS last_bpm_distance_avg_2,
        duration_sec_decile - LAG(duration_sec_decile) OVER (ORDER BY bpm_distance, id) AS duration_sec_decile_change
    FROM metrics
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm_distance,
        duration_sec_vs_artist_id_avg,
        duration_sec_rank,
        duration_sec_decile,
        artist_id_avg_duration_sec,
        last_bpm_distance_avg_2,
        duration_sec_decile_change,
        CONCAT_WS(' / ', release_date_label, bpm_tier) AS release_date_label_bpm_tier_label
    FROM sequenced
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM combined_details
    WHERE duration_sec_rank <= 5
      AND duration_sec_vs_artist_id_avg >= -84.0
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm_distance,
    duration_sec_vs_artist_id_avg,
    duration_sec_rank,
    duration_sec_decile,
    last_bpm_distance_avg_2,
    duration_sec_decile_change,
    release_date_label_bpm_tier_label,
    artist_genre,
    artist_debut_year,
    artist_country,
    CONCAT_WS(': ', artist_genre, artist_country) AS artist_genre_artist_country_label
FROM top_rows
WHERE last_bpm_distance_avg_2 > 27.0
ORDER BY duration_sec_decile_change DESC
