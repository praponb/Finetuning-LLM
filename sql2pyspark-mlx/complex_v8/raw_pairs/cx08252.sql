WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE duration_sec BETWEEN 165 AND 357
      AND NOT is_explicit
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        artist_id,
        MAX(release_date) AS last_release_date,
        COUNT(*) AS cnt,
        ROUND(AVG(moods_count), 2) AS avg_moods_count
    FROM filtered_tracks
    WHERE release_date >= DATE '2024-06-03'
    GROUP BY 1, 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        release_date_ym,
        artist_id,
        last_release_date,
        cnt,
        avg_moods_count,
        MAX(cnt) OVER (PARTITION BY MONTH(last_release_date)) AS last_release_date_month_max_cnt,
        ROUND(avg_moods_count - AVG(avg_moods_count) OVER (PARTITION BY MONTH(last_release_date)), 2) AS avg_moods_count_vs_last_release_date_month_avg,
        RANK() OVER w_all_by_last_release_date_tie AS last_release_date_rank,
        ROUND(AVG(cnt) OVER (ORDER BY last_release_date, release_date_ym, artist_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS cnt_avg_1_prior
    FROM per_group
    WINDOW w_all_by_last_release_date_tie AS (ORDER BY last_release_date)
)
SELECT release_date_ym, artist_id, metric, value
FROM (
    SELECT release_date_ym, artist_id, avg_moods_count_vs_last_release_date_month_avg, cnt_avg_1_prior, avg_moods_count
    FROM with_windows
) src
UNPIVOT (
    value FOR metric IN (avg_moods_count_vs_last_release_date_month_avg AS m_avg_moods_count_vs_last_release_date_month_avg, cnt_avg_1_prior AS m_cnt_avg_1_prior, avg_moods_count AS m_avg_moods_count)
) unp
ORDER BY release_date_ym, artist_id, metric
