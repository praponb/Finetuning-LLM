WITH prepared AS (
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
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE (artist_id IS NOT NULL OR SIZE(moods) >= 2)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        release_date,
        moods,
        title_prefix,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        SUM(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_sum_7,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        COUNT(*) OVER w_artist_id_is_explicit AS rows_in_artist_id_is_explicit,
        MAX(duration_sec) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)) AS artist_id_release_date_month_max_duration_sec,
        ROUND(AVG(bpm) OVER w_artist_id, 2) AS artist_id_avg_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id), 3) AS release_date_percent_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS artist_id_is_explicit_avg_duration_sec,
        ROUND(CUME_DIST() OVER (ORDER BY release_date, id), 3) AS release_date_cume_dist,
        NTILE(4) OVER w_is_explicit_artist_id_by_rel AS release_date_quartile,
        RANK() OVER w_is_explicit_by_release_date_3 AS release_date_rank,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS release_date_dense_rank,
        ROUND(CUME_DIST() OVER w_artist_id_is_explicit_by_bpm, 3) AS bpm_cume_dist,
        DATEDIFF(release_date, LAG(release_date) OVER w_all_by_release_date) AS days_since_prev_release_date
    FROM prepared
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_id AS (PARTITION BY artist_id), w_is_explicit_artist_id_by_rel AS (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id), w_is_explicit_by_release_date_3 AS (PARTITION BY is_explicit ORDER BY release_date DESC), w_artist_id_is_explicit_by_bpm AS (PARTITION BY artist_id, is_explicit ORDER BY bpm, id), w_all_by_release_date AS (ORDER BY release_date, id)
),
totals AS (
    -- Summarise per group
    SELECT
        release_date_quartile,
        SUM(rows_in_artist_id_is_explicit) FILTER (WHERE LENGTH(TRIM(title)) >= 8) AS rows_in_artist_id_is_explicit_length_trim_title_,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        SUM(bpm_avg_7) AS total_bpm_avg_7
    FROM ranked
    GROUP BY 1
    HAVING MAX(duration_sec_vs_artist_id_is_explicit_avg) > -294.0
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY release_date_quartile ORDER BY rows_in_artist_id_is_explicit_length_trim_title_ NULLS LAST) AS rows_in_artist_id_is_explicit_length_trim_title__2,
        MIN(total_bpm_avg_7) OVER w_release_date_quartile AS release_date_quartile_min_total_bpm_avg_7
    FROM totals
    WINDOW w_release_date_quartile AS (PARTITION BY release_date_quartile)
)
SELECT
    release_date_quartile,
    rows_in_artist_id_is_explicit_length_trim_title_,
    title_list,
    total_bpm_avg_7,
    rows_in_artist_id_is_explicit_length_trim_title__2
FROM sequenced
WHERE total_bpm_avg_7 >= 468.0
ORDER BY total_bpm_avg_7 DESC
