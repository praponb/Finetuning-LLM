WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE (artist_id IS NOT NULL OR duration_sec <= 353)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        LEAD(duration_sec) OVER w_is_explicit_release_date_yea AS next_duration_sec,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit, YEAR(release_date) ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        ROW_NUMBER() OVER w_is_explicit_release_date_yea AS rn,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id, 2) AS duration_sec_vs_artist_id_avg,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        ROUND(CUME_DIST() OVER w_is_explicit_release_date_yea, 3) AS release_date_cume_dist,
        LEAD(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC, id) AS next_duration_sec_2,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist,
        NTILE(4) OVER w_artist_id_by_release_date AS release_date_quartile,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2
    FROM prepared_tracks
    WINDOW w_is_explicit_release_date_yea AS (PARTITION BY is_explicit, YEAR(release_date) ORDER BY release_date DESC NULLS FIRST, id), w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        title_prefix,
        rows_in_artist_id,
        second_release_date,
        rn,
        duration_sec_vs_artist_id_avg,
        artist_id_max_duration_sec,
        release_date_cume_dist,
        next_duration_sec_2,
        duration_sec_cume_dist,
        release_date_quartile,
        duration_sec_avg_2,
        ROUND(AVG(rows_in_artist_id) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_rows_in_artist_id,
        ROUND(release_date_quartile / NULLIF(SUM(release_date_quartile) OVER (PARTITION BY artist_id), 0), 4) AS release_date_quartile_share_of_artist_id,
        COUNT(*) OVER (PARTITION BY artist_id ORDER BY duration_sec_cume_dist NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM metrics
)
SELECT
    id,
    release_date,
    title_prefix,
    rows_in_artist_id,
    release_date_cume_dist,
    next_duration_sec_2,
    duration_sec_cume_dist,
    release_date_quartile,
    duration_sec_avg_2,
    release_date_quartile_share_of_artist_id,
    running_count,
    LOWER(TRIM(title)) AS title_clean
FROM with_windows
ORDER BY duration_sec_cume_dist DESC, id
LIMIT 10
