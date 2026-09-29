WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
        LAST_DAY(release_date) AS release_date_month_end
    FROM tracks
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        DATEDIFF(release_date_month_end, LAG(release_date_month_end) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date, id)) AS days_since_prev_release_date_month_end,
        ROW_NUMBER() OVER w_release_date_month_artist_id AS release_date_seq,
        SUM(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        ROUND(AVG(bpm) OVER w_artist_id, 2) AS artist_id_avg_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)), 2) AS artist_id_release_date_month_avg_bpm,
        MAX(bpm) OVER (ORDER BY release_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_max_4,
        MAX(bpm) OVER (ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        ROUND(AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        FIRST_VALUE(release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS bpm_change,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY bpm ASC, id)) AS days_since_prev_release_date,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)), 0), 4) AS bpm_share_of_artist_id_release_date_month,
        RANK() OVER w_release_date_month_artist_id_5 AS release_date_rank
    FROM recent
    WINDOW w_release_date_month_artist_id AS (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date, id), w_artist_id AS (PARTITION BY artist_id), w_release_date_month_artist_id_5 AS (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date)
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    title_ref,
    release_date_month_end,
    bpm_share_of_artist_id,
    days_since_prev_release_date_month_end,
    release_date_seq,
    artist_id_avg_bpm,
    artist_id_release_date_month_avg_bpm,
    bpm_max_4,
    max_bpm_so_far,
    first_release_date,
    bpm_change,
    days_since_prev_release_date,
    bpm_share_of_artist_id_release_date_month,
    release_date_rank,
    artist_id_avg_bpm - release_date_seq AS artist_id_avg_bpm_minus_release_date_seq,
    LOWER(REGEXP_REPLACE(TRIM(title_ref), ' ', '_')) AS title_ref_slug
FROM metrics
ORDER BY days_since_prev_release_date_month_end DESC NULLS FIRST
