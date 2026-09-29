WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'N%'
      AND ARRAY_CONTAINS(moods, 'focus')
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_2_prior,
        artist_debut_year - LAG(artist_debut_year) OVER (PARTITION BY artist_country ORDER BY bpm ASC, id) AS artist_debut_year_change,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_country ORDER BY bpm ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_id), 2) AS artist_debut_year_vs_artist_id_avg,
        MAX(bpm) OVER w_artist_id AS artist_id_max_bpm,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id)) AS days_since_prev_release_date,
        ROUND(AVG(artist_debut_year) OVER w_artist_id, 2) AS artist_id_avg_artist_debut_year,
        COUNT(*) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec AS seq_no,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_country ORDER BY bpm ASC, id) AS duration_sec_change,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        NTILE(5) OVER w_artist_id_by_artist_debut_ye_2 AS artist_debut_year_quintile,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER w_artist_id, 0), 4) AS artist_debut_year_share_of_artist_id
    FROM joined
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec DESC, id), w_artist_id_by_artist_debut_ye_2 AS (PARTITION BY artist_id ORDER BY artist_debut_year DESC, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(last_known_release_date) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_release_date,
        RANK() OVER w_artist_country_artist_is_ver AS bpm_rank,
        DENSE_RANK() OVER (ORDER BY duration_sec ASC) AS duration_sec_dense_rank,
        MAX(duration_sec) OVER (PARTITION BY artist_debut_year_quintile, seq_no) AS artist_debut_year_quintile_seq_no_max_duration_s
    FROM metrics
    WINDOW w_artist_country_artist_is_ver AS (PARTITION BY artist_country, artist_is_verified ORDER BY bpm DESC)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        SUM(artist_id_max_bpm) AS total_artist_id_max_bpm,
        ROUND(SUM(artist_debut_year_avg_2_prior) / NULLIF(SUM(running_bpm), 0), 3) AS artist_debut_year_avg_2_prior_to_running_bpm_rat,
        MAX(days_since_prev_release_date) AS max_days_since_prev_release_date
    FROM with_windows
    GROUP BY DATE_FORMAT(release_date, 'yyyy-MM')
    HAVING AVG(bpm_rank) > 3
)
SELECT
    release_date_ym,
    total_artist_id_max_bpm,
    artist_debut_year_avg_2_prior_to_running_bpm_rat,
    COALESCE(total_artist_id_max_bpm, 0) AS total_artist_id_max_bpm_filled
FROM rollup_base
