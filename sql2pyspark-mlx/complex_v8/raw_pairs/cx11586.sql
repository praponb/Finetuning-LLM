WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'sad')
      AND NOT is_explicit
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 2) AS bpm_vs_release_date_month_avg,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        RANK() OVER w_is_explicit_by_bpm_ties AS bpm_rank,
        FIRST_VALUE(bpm) OVER (PARTITION BY is_explicit, MONTH(release_date) ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id), 3) AS release_date_cume_dist,
        DATEDIFF(release_date, LAG(release_date) OVER w_is_explicit_release_date_mon_2) AS days_since_prev_release_date,
        MIN(bpm) OVER (PARTITION BY artist_id) AS artist_id_min_bpm,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm
    FROM cleaned
    WINDOW w_is_explicit_by_bpm_ties AS (PARTITION BY is_explicit ORDER BY bpm), w_is_explicit_release_date_mon_2 AS (PARTITION BY is_explicit, MONTH(release_date) ORDER BY bpm, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        bpm_vs_release_date_month_avg,
        second_release_date,
        artist_id_avg_bpm,
        first_bpm,
        release_date_cume_dist,
        days_since_prev_release_date,
        artist_id_min_bpm,
        last_known_bpm
    FROM windowed
    WHERE bpm_rank <= 3
)
SELECT
    id,
    artist_id,
    release_date,
    bpm_vs_release_date_month_avg,
    second_release_date,
    artist_id_avg_bpm,
    first_bpm,
    release_date_cume_dist,
    artist_id_min_bpm,
    last_known_bpm,
    DATE_FORMAT(second_release_date, 'MMM yyyy') AS second_release_date_label,
    DATEDIFF(DATE '2025-12-31', second_release_date) AS days_since_second_release_date
FROM top_rows
ORDER BY second_release_date_label DESC NULLS FIRST, artist_id_min_bpm, id
LIMIT 5
