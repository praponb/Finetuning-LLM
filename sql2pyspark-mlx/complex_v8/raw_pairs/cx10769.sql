WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE bpm BETWEEN 89 AND 170
      AND is_explicit = FALSE
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.days_since_release_date,
        a.is_verified AS artist_is_verified
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_is_verified, artist_id) AS rows_in_artist_is_verified_artist_id,
        LAG(days_since_release_date, 3, 0) OVER w_artist_is_verified_by_days_s AS prev3_days_since_release_date,
        MIN(days_since_release_date) OVER w_artist_is_verified AS artist_is_verified_min_days_since_release_date,
        MAX(days_since_release_date) OVER w_artist_is_verified_artist_id AS artist_is_verified_artist_id_max_days_since_rele,
        LAG(days_since_release_date, 1, 0) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY days_since_release_date, id) AS prev_days_since_release_date,
        ROUND(days_since_release_date / NULLIF(SUM(days_since_release_date) OVER (PARTITION BY artist_is_verified, artist_id), 0), 4) AS days_since_release_date_share_of_artist_is_verif,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified ORDER BY days_since_release_date DESC), 3) AS days_since_release_date_percent_rank
    FROM combined_details_artist
    WINDOW w_artist_is_verified_by_days_s AS (PARTITION BY artist_is_verified ORDER BY days_since_release_date DESC, id), w_artist_is_verified AS (PARTITION BY artist_is_verified), w_artist_is_verified_artist_id AS (PARTITION BY artist_is_verified, artist_id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        moods,
        days_since_release_date,
        artist_is_verified,
        rows_in_artist_is_verified_artist_id,
        artist_is_verified_min_days_since_release_date,
        artist_is_verified_artist_id_max_days_since_rele,
        days_since_release_date_share_of_artist_is_verif,
        days_since_release_date_percent_rank,
        FIRST_VALUE(days_since_release_date) OVER (ORDER BY days_since_release_date_share_of_artist_is_verif DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_release_date,
        SUM(prev_days_since_release_date) OVER (ORDER BY days_since_release_date_share_of_artist_is_verif DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_days_since_release_date_sum_6_prior,
        ROUND(days_since_release_date - AVG(days_since_release_date) OVER (PARTITION BY artist_is_verified, artist_id), 2) AS days_since_release_date_vs_artist_is_verified_ar,
        ROUND(AVG(days_since_release_date) OVER (PARTITION BY artist_is_verified, artist_id), 2) AS artist_is_verified_artist_id_avg_days_since_rele,
        ROW_NUMBER() OVER (ORDER BY days_since_release_date_share_of_artist_is_verif DESC, id) AS days_since_release_date_share_of_artist_is_verif_2
    FROM with_trends
)
-- Final projection
SELECT
    id,
    moods,
    days_since_release_date,
    artist_is_verified,
    rows_in_artist_is_verified_artist_id,
    artist_is_verified_min_days_since_release_date,
    artist_is_verified_artist_id_max_days_since_rele,
    days_since_release_date_share_of_artist_is_verif,
    days_since_release_date_percent_rank,
    prev_days_since_release_date_sum_6_prior,
    days_since_release_date_vs_artist_is_verified_ar,
    artist_is_verified_artist_id_avg_days_since_rele,
    days_since_release_date_share_of_artist_is_verif_2,
    IF(artist_is_verified, 'yes', 'no') AS artist_is_verified_label,
    COALESCE(days_since_release_date, 0) AS days_since_release_date_filled
FROM windowed
ORDER BY days_since_release_date_filled DESC
