WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date_2
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND NOT is_explicit
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        days_since_release_date,
        days_since_release_date_2,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY MONTH(release_date), artist_id), 0), 4) AS duration_sec_share_of_release_date_month_artist_,
        MAX(duration_sec) OVER (PARTITION BY artist_id ORDER BY days_since_release_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_max_3_prior,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS duration_sec_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY days_since_release_date, id), 3) AS days_since_release_date_percent_rank,
        ROUND(days_since_release_date_2 - AVG(days_since_release_date_2) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 2) AS days_since_release_date_2_vs_release_date_month_,
        ROUND(AVG(days_since_release_date_2) OVER (PARTITION BY MONTH(release_date), artist_id), 2) AS release_date_month_artist_id_avg_days_since_rele,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date))) AS distinct_artist_id_in_release_date_month,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, MONTH(release_date)), 0), 4) AS duration_sec_share_of_artist_id_release_date_mon,
        FIRST_VALUE(days_since_release_date_2) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_release_date_2,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm DESC, id)) AS days_since_prev_release_date,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        MAX(days_since_release_date) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_release_date_max_1_prior,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_release_date_ti, 3) AS release_date_percent_rank,
        COUNT(*) OVER (PARTITION BY artist_id, MONTH(release_date)) AS rows_in_artist_id_release_date_month
    FROM base
    WINDOW w_artist_id_by_release_date_ti AS (PARTITION BY artist_id ORDER BY release_date DESC)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        SIZE(moods) AS moods_count,
        DATE_ADD(release_date, 90) AS release_date_plus_90d
    FROM with_trends
    WHERE release_date_percent_rank <= 0.7
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY moods_count ORDER BY days_since_release_date_2_vs_release_date_month_ DESC, id) AS days_since_release_date_2_vs_release_date_month__2,
        COUNT(*) OVER (PARTITION BY artist_id, moods_count) AS rows_in_artist_id_moods_count,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY moods_count ORDER BY days_since_release_date_2_vs_release_date_month_ DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        LAST_VALUE(days_since_prev_release_date) OVER (ORDER BY duration_sec_max_3_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_prev_release_date
    FROM annotated
)
-- Final projection
SELECT DISTINCT id, days_since_release_date_max_1_prior, artist_id
FROM with_trends_2
ORDER BY id
