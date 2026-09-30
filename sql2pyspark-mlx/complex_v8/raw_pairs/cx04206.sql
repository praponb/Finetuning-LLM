WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ABS(duration_sec - 352) AS duration_sec_distance,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE duration_sec < 312
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        duration_sec_distance,
        title_prefix,
        ROUND(CUME_DIST() OVER w_release_date_month_is_explic, 3) AS duration_sec_distance_cume_dist,
        ROUND(AVG(duration_sec) OVER (PARTITION BY MONTH(release_date), is_explicit ORDER BY duration_sec_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id
    FROM prepared_tracks
    WINDOW w_release_date_month_is_explic AS (PARTITION BY MONTH(release_date), is_explicit ORDER BY duration_sec_distance DESC, id), w_artist_id AS (PARTITION BY artist_id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        duration_sec_distance,
        title_prefix,
        duration_sec_distance_cume_dist,
        duration_sec_avg_7,
        ROUND(duration_sec / NULLIF(duration_sec_distance, 0), 3) AS duration_sec_per_duration_sec_distance,
        UPPER(TRIM(title_prefix)) AS title_prefix_clean
    FROM with_windows
    WHERE title_prefix IS NOT NULL
),
derived_2 AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN duration_sec_distance >= 273 THEN 'large' WHEN duration_sec_distance >= 147 THEN 'medium' ELSE 'small' END AS duration_sec_distance_tier
    FROM derived
)
SELECT
    artist_id,
    COALESCE(duration_sec_distance_tier, 'ALL') AS duration_sec_distance_tier,
    is_explicit,
    COUNT(*) AS cnt
FROM derived_2
GROUP BY GROUPING SETS ((artist_id, duration_sec_distance_tier, is_explicit), (artist_id))
ORDER BY artist_id NULLS LAST, duration_sec_distance_tier NULLS LAST, is_explicit NULLS LAST
