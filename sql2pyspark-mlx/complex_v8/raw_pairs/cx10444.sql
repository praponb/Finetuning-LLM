WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE SIZE(moods) >= 1
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.release_date_label,
        a.is_verified AS artist_is_verified
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        moods,
        release_date_label,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_2_prior,
        SUM(duration_sec) OVER (PARTITION BY artist_id, artist_is_verified) AS artist_id_artist_is_verified_sum_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, artist_is_verified), 0), 4) AS duration_sec_share_of_artist_id_artist_is_verifi,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4,
        ROUND(CUME_DIST() OVER w_all_by_duration_sec, 3) AS duration_sec_cume_dist,
        COUNT(*) OVER w_artist_id_artist_is_verified AS rows_in_artist_id_artist_is_verified,
        MAX(duration_sec) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_sec_max_2,
        FIRST_VALUE(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_is_verified ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist_2,
        NTILE(5) OVER w_artist_is_verified_by_durati_4 AS duration_sec_quintile
    FROM enriched_artist
    WINDOW w_artist_id_artist_is_verified AS (PARTITION BY artist_id, artist_is_verified), w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id), w_artist_is_verified_by_durati_4 AS (PARTITION BY artist_is_verified ORDER BY duration_sec DESC, id)
)
-- Final projection
SELECT
    id,
    title,
    release_date_label,
    duration_sec_avg_2_prior,
    duration_sec_share_of_artist_id_artist_is_verifi,
    duration_sec_avg_4,
    duration_sec_cume_dist,
    duration_sec_max_2,
    first_duration_sec,
    duration_sec_quintile,
    INITCAP(TRIM(release_date_label)) AS release_date_label_clean,
    CONCAT(release_date_label, '#', CAST(first_duration_sec AS STRING)) AS release_date_label_ref
FROM with_trends
WHERE ARRAY_CONTAINS(moods, 'sad')
ORDER BY duration_sec_avg_4 DESC, duration_sec_cume_dist DESC
