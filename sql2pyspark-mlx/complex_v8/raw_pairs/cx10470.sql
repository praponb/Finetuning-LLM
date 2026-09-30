WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.bpm_per_duration_sec,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
    WHERE LOWER(a.artist_name) LIKE '%pa%'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        bpm_per_duration_sec,
        artist_debut_year,
        ROUND(CUME_DIST() OVER (ORDER BY bpm_per_duration_sec DESC, id), 3) AS bpm_per_duration_sec_cume_dist,
        MIN(artist_debut_year) OVER (PARTITION BY artist_id) AS artist_id_min_artist_debut_year,
        ROUND(CUME_DIST() OVER w_artist_id_by_artist_debut_ye, 3) AS artist_debut_year_cume_dist,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY bpm_per_duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_3_prior,
        NTH_VALUE(bpm_per_duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY artist_debut_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_per_duration_sec,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_bpm_per_duratio_2, 3) AS bpm_per_duration_sec_percent_rank,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        LEAD(bpm_per_duration_sec, 1, 0) OVER w_artist_id_by_bpm_per_duratio_3 AS next_bpm_per_duration_sec,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_id ORDER BY bpm_per_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        ROUND(bpm_per_duration_sec / NULLIF(SUM(bpm_per_duration_sec) OVER w_artist_id, 0), 4) AS bpm_per_duration_sec_share_of_artist_id
    FROM enriched_artist
    WINDOW w_artist_id_by_artist_debut_ye AS (PARTITION BY artist_id ORDER BY artist_debut_year ASC NULLS LAST), w_artist_id_by_bpm_per_duratio_2 AS (PARTITION BY artist_id ORDER BY bpm_per_duration_sec DESC), w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_bpm_per_duratio_3 AS (PARTITION BY artist_id ORDER BY bpm_per_duration_sec DESC, id)
)
SELECT
    id,
    artist_id,
    bpm_per_duration_sec,
    artist_debut_year,
    bpm_per_duration_sec_cume_dist,
    artist_id_min_artist_debut_year,
    artist_debut_year_cume_dist,
    artist_debut_year_avg_3_prior,
    bpm_per_duration_sec_percent_rank,
    rows_in_artist_id,
    next_bpm_per_duration_sec,
    second_artist_debut_year,
    bpm_per_duration_sec_share_of_artist_id,
    bpm_per_duration_sec - second_artist_debut_year AS bpm_per_duration_sec_minus_second_artist_debut_y
FROM with_trends
ORDER BY second_artist_debut_year DESC
