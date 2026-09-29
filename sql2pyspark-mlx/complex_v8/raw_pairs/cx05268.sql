WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 279) AS duration_sec_distance,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND bpm > 100
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.duration_sec_distance,
        s.release_date_label,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        moods,
        artist_is_verified,
        duration_sec_distance - LAG(duration_sec_distance) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year ASC NULLS LAST, id) AS duration_sec_distance_change,
        SUM(bpm) OVER (PARTITION BY artist_id, artist_is_verified) AS artist_id_artist_is_verified_sum_bpm,
        SIZE(COLLECT_SET(artist_id) OVER w_artist_is_verified) AS distinct_artist_id_in_artist_is_verified,
        RANK() OVER (PARTITION BY artist_id ORDER BY bpm DESC) AS bpm_rank,
        MIN(bpm) OVER (PARTITION BY artist_id, artist_is_verified) AS artist_id_artist_is_verified_min_bpm,
        SUM(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_artist_debut_year,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_bpm, 3) AS bpm_percent_rank,
        MIN(artist_debut_year) OVER w_artist_id_artist_is_verified_2 AS artist_id_artist_is_verified_min_artist_debut_ye,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year ASC NULLS LAST, id) AS bpm_change,
        NTILE(10) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS bpm_decile,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_is_verified), 2) AS artist_debut_year_vs_artist_is_verified_avg,
        SUM(duration_sec_distance) OVER (PARTITION BY artist_id, artist_is_verified) AS artist_id_artist_is_verified_sum_duration_sec_di,
        RANK() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY artist_debut_year DESC, id) AS artist_debut_year_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY artist_debut_year ASC NULLS LAST, id) AS rn
    FROM enriched
    WINDOW w_artist_is_verified AS (PARTITION BY artist_is_verified), w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm, id), w_artist_id_artist_is_verified_2 AS (PARTITION BY artist_id, artist_is_verified)
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(bpm_change / NULLIF(bpm_percent_rank, 0), 3) AS bpm_change_per_bpm_percent_rank,
        ROUND(rn / NULLIF(bpm_change, 0), 3) AS rn_per_bpm_change
    FROM ranked
),
totals AS (
    -- Summarise per group
    SELECT
        bpm_decile,
        SUM(bpm_change) FILTER (WHERE artist_is_verified = TRUE) AS bpm_change_artist_is_verified_true,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(bpm_decile) AS min_bpm_decile,
        MIN(bpm_change_per_bpm_percent_rank) AS min_bpm_change_per_bpm_percent_rank
    FROM enriched_2
    GROUP BY bpm_decile
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids,
        LAG(bpm_decile, 2) OVER (ORDER BY bpm_change_artist_is_verified_true, bpm_decile) AS prev2_bpm_decile
    FROM totals
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
-- Final projection
SELECT
    bpm_decile,
    bpm_change_artist_is_verified_true,
    distinct_artist_ids,
    distinct_ids,
    min_bpm_decile,
    prev2_bpm_decile,
    row_count + prev2_bpm_decile AS row_count_plus_prev2_bpm_decile,
    FLOOR(row_count / 2) * 2 AS row_count_bucket
FROM ranked_2
