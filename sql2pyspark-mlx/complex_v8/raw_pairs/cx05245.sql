WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'R%'
      AND artist_id <= 11
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        least_bpm_duration_sec,
        ROUND(CUME_DIST() OVER (ORDER BY least_bpm_duration_sec ASC), 3) AS least_bpm_duration_sec_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec, id), 3) AS duration_sec_percent_rank,
        NTILE(5) OVER w_is_explicit_artist_id_by_bpm AS bpm_quintile,
        SUM(least_bpm_duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_sum_least_bpm_duration_sec,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_percent_rank_2,
        COUNT(*) OVER w_is_explicit AS rows_in_is_explicit,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC), 3) AS release_date_percent_rank,
        LAG(bpm, 3) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id) AS prev3_bpm,
        bpm - LAG(bpm) OVER w_artist_id_is_explicit_by_rel_2 AS bpm_change,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec, id) AS duration_sec_rank,
        ROUND(least_bpm_duration_sec / NULLIF(SUM(least_bpm_duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS least_bpm_duration_sec_share_of_artist_id
    FROM base
    WINDOW w_is_explicit_artist_id_by_bpm AS (PARTITION BY is_explicit, artist_id ORDER BY bpm, id), w_is_explicit AS (PARTITION BY is_explicit), w_artist_id_is_explicit_by_rel_2 AS (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(least_bpm_duration_sec - AVG(least_bpm_duration_sec) OVER (PARTITION BY bpm_quintile, duration_sec_rank), 2) AS least_bpm_duration_sec_vs_bpm_quintile_duration_,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec_percent_rank_2 DESC), 3) AS duration_sec_percent_rank_2_percent_rank,
        ROUND(prev3_bpm - AVG(prev3_bpm) OVER (PARTITION BY bpm_quintile, duration_sec_rank), 2) AS prev3_bpm_vs_bpm_quintile_duration_sec_rank_avg
    FROM with_trends
),
summary AS (
    -- Summarise per group
    SELECT
        bpm_quintile,
        duration_sec_rank,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        MIN(release_date) AS first_release_date,
        COUNT(*) AS n_rows,
        MAX(release_date) AS last_release_date,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true
    FROM metrics
    GROUP BY bpm_quintile, duration_sec_rank, DATE_FORMAT(release_date, 'yyyy-MM')
)
-- Final projection
SELECT
    bpm_quintile,
    duration_sec_rank,
    release_date_ym,
    first_release_date,
    n_rows,
    last_release_date,
    n_is_explicit_true
FROM summary
ORDER BY n_is_explicit_true
