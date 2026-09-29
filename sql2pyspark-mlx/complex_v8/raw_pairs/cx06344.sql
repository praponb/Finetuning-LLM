WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        a.country AS artist_country
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_country), 2) AS bpm_vs_artist_country_avg,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_country, 0), 4) AS bpm_share_of_artist_country,
        LEAD(bpm, 1, 0) OVER w_artist_country_by_duration_s AS next_bpm,
        ROW_NUMBER() OVER (PARTITION BY artist_country ORDER BY duration_sec DESC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_artist_country_by_duration_s_2, 3) AS duration_sec_percent_rank,
        LAG(days_since_release_date, 3) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm, id) AS prev3_days_since_release_date,
        ROUND(days_since_release_date / NULLIF(SUM(days_since_release_date) OVER (PARTITION BY artist_country), 0), 4) AS days_since_release_date_share_of_artist_country,
        MIN(days_since_release_date) OVER (PARTITION BY artist_country ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_release_date_so_far
    FROM with_details
    WINDOW w_artist_country AS (PARTITION BY artist_country), w_artist_country_by_duration_s AS (PARTITION BY artist_country ORDER BY duration_sec DESC, id), w_artist_country_by_duration_s_2 AS (PARTITION BY artist_country ORDER BY duration_sec, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        bpm,
        moods,
        days_since_release_date,
        artist_country,
        bpm_vs_artist_country_avg,
        bpm_share_of_artist_country,
        next_bpm,
        duration_sec_percent_rank,
        days_since_release_date_share_of_artist_country,
        min_days_since_release_date_so_far,
        RANK() OVER w_all_by_duration_sec AS duration_sec_rank,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec_percent_rank ASC, id), 3) AS duration_sec_percent_rank_cume_dist,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm_vs_artist_country_avg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        SUM(days_since_release_date) OVER (PARTITION BY artist_id ORDER BY bpm_vs_artist_country_avg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_release_date,
        bpm - LAG(bpm) OVER (ORDER BY duration_sec ASC, id) AS bpm_change,
        SIZE(COLLECT_SET(artist_country) OVER w_artist_id) AS distinct_artist_country_in_artist_id,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY bpm_vs_artist_country_avg NULLS LAST) AS bpm_vs_artist_country_avg_dense_rank
    FROM with_windows
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec ASC, id), w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    bpm,
    moods,
    days_since_release_date,
    artist_country,
    bpm_vs_artist_country_avg,
    bpm_share_of_artist_country,
    days_since_release_date_share_of_artist_country,
    min_days_since_release_date_so_far,
    duration_sec_percent_rank_cume_dist,
    last_known_bpm,
    bpm_share_of_artist_id,
    running_days_since_release_date,
    bpm_change,
    distinct_artist_country_in_artist_id,
    bpm_vs_artist_country_avg_dense_rank,
    COALESCE(artist_country, 'UNKNOWN') AS artist_country_or_unknown
FROM ranked
WHERE distinct_artist_country_in_artist_id <= 7
ORDER BY bpm_change DESC, bpm_share_of_artist_country DESC NULLS FIRST
