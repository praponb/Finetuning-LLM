WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id AND UPPER(TRIM(a.artist_name)) LIKE 'S%'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_genre ORDER BY bpm NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        ROUND(CUME_DIST() OVER w_artist_is_verified_artist_id, 3) AS duration_sec_cume_dist,
        COUNT(*) OVER w_artist_country_artist_is_ver AS rows_in_artist_country_artist_is_verified,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_id, artist_country ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        duration_sec - LAG(duration_sec) OVER w_artist_genre_by_bpm AS duration_sec_change,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_country, artist_is_verified), 2) AS artist_country_artist_is_verified_avg_duration_s,
        LEAD(duration_sec, 3, 0) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec DESC, id) AS next3_duration_sec,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, artist_country ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROW_NUMBER() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec DESC, id) AS rn,
        COUNT(*) OVER (PARTITION BY artist_is_verified) AS rows_in_artist_is_verified,
        RANK() OVER (ORDER BY bpm DESC) AS bpm_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_is_verified), 2) AS duration_sec_vs_artist_is_verified_avg
    FROM with_details
    WINDOW w_artist_is_verified_artist_id AS (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec DESC), w_artist_country_artist_is_ver AS (PARTITION BY artist_country, artist_is_verified), w_artist_genre_by_bpm AS (PARTITION BY artist_genre ORDER BY bpm NULLS LAST, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        duration_sec_avg_2,
        rows_in_artist_country_artist_is_verified,
        last_duration_sec,
        duration_sec_change,
        next3_duration_sec,
        rn,
        rows_in_artist_is_verified,
        bpm_rank,
        duration_sec_vs_artist_is_verified_avg,
        LOWER(TRIM(title)) AS title_clean
    FROM with_trends
    WHERE bpm_rank >= 2
),
totals AS (
    -- Group-level totals
    SELECT
        artist_id,
        CASE WHEN duration_sec_change >= 126 THEN 'HIGH' WHEN duration_sec_change >= -210 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_change_tier,
        bpm_rank,
        MIN(bpm_rank) AS min_bpm_rank,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(*) AS n_rows
    FROM tagged
    GROUP BY artist_id, CASE WHEN duration_sec_change >= 126 THEN 'HIGH' WHEN duration_sec_change >= -210 THEN 'MEDIUM' ELSE 'LOW' END, bpm_rank
)
SELECT
    artist_id,
    duration_sec_change_tier,
    bpm_rank,
    n_rows
FROM totals
ORDER BY artist_id DESC
