WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        INITCAP(TRIM(title)) AS title_clean,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'sad')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        duration_sec,
        bpm,
        title_clean,
        bpm - LAG(bpm) OVER (PARTITION BY MONTH(release_date) ORDER BY bpm NULLS LAST, id) AS bpm_change,
        ROUND(AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 2) AS release_date_month_avg_bpm,
        LEAD(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS next2_bpm,
        LAG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS prev_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY YEAR(release_date), artist_id), 0), 4) AS bpm_share_of_release_date_year_artist_id,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROW_NUMBER() OVER w_artist_id_by_bpm_2 AS bpm_seq,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_bpm, 3) AS bpm_percent_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY YEAR(release_date), artist_id), 2) AS duration_sec_vs_release_date_year_artist_id_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 0), 4) AS duration_sec_share_of_release_date_month,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm), 3) AS bpm_percent_rank_2,
        COUNT(*) OVER (PARTITION BY YEAR(release_date), artist_id) AS rows_in_release_date_year_artist_id
    FROM recent
    WINDOW w_artist_id_by_bpm_2 AS (PARTITION BY artist_id ORDER BY bpm, id), w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm DESC, id)
)
-- Final projection
SELECT
    id,
    duration_sec,
    bpm,
    bpm_change,
    release_date_month_avg_bpm,
    next2_bpm,
    prev_bpm,
    bpm_share_of_release_date_year_artist_id,
    second_bpm,
    duration_sec_share_of_release_date_month,
    rows_in_release_date_year_artist_id
FROM sequenced
ORDER BY bpm_change
