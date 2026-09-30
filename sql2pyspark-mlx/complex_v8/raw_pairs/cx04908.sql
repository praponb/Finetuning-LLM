WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE release_date < DATE '2024-12-22'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_per_bpm,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_duration_sec_per_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_2,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        ROUND(duration_sec_per_bpm / NULLIF(SUM(duration_sec_per_bpm) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_per_bpm_share_of_artist_id_is_expli,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_per_bpm ASC), 3) AS duration_sec_per_bpm_percent_rank,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS duration_sec_per_bpm_vs_is_explicit_artist_id_av,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        NTILE(4) OVER (ORDER BY duration_sec, id) AS duration_sec_quartile,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_1_prior
    FROM prepared
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_per_bpm,
        last_known_release_date,
        is_explicit_artist_id_avg_duration_sec_per_bpm,
        rows_in_artist_id_is_explicit,
        first_release_date,
        duration_sec_per_bpm_percent_rank,
        duration_sec_per_bpm_vs_is_explicit_artist_id_av,
        rows_in_is_explicit_artist_id,
        ABS(duration_sec_quartile - 4) AS duration_sec_quartile_distance,
        DATE_ADD(last_known_release_date, 90) AS last_known_release_date_plus_90d,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM with_windows
    WHERE rows_in_artist_id_is_explicit > 7
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(last_known_release_date, 'yyyy-MM') AS last_known_release_date_ym,
        duration_sec_quartile_distance,
        is_explicit,
        ROUND(AVG(duration_sec_quartile_distance), 2) AS avg_duration_sec_quartile_distance,
        ROUND(100 * COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_explicit_true,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(bpm), 2) AS stddev_bpm
    FROM tagged
    GROUP BY DATE_FORMAT(last_known_release_date, 'yyyy-MM'), duration_sec_quartile_distance, is_explicit
)
-- Final projection
SELECT
    last_known_release_date_ym,
    duration_sec_quartile_distance,
    is_explicit,
    avg_duration_sec_quartile_distance,
    pct_is_explicit_true,
    n_is_explicit_true,
    distinct_ids,
    stddev_bpm,
    ROUND(distinct_ids / NULLIF(pct_is_explicit_true, 0), 3) AS distinct_ids_per_pct_is_explicit_true
FROM stats
ORDER BY duration_sec_quartile_distance, distinct_ids_per_pct_is_explicit_true DESC NULLS FIRST
