WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 142 THEN 'HIGH' WHEN bpm >= 104 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE NOT is_explicit
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, bpm_tier), 2) AS bpm_vs_artist_id_bpm_tier_avg,
        ROUND(CUME_DIST() OVER w_bpm_tier_by_duration_sec_tie, 3) AS duration_sec_cume_dist,
        SUM(bpm) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        LEAD(release_date, 3) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY release_date ASC, id) AS next3_release_date,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        ROUND(AVG(bpm) OVER (ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_4,
        duration_sec - LAG(duration_sec) OVER (ORDER BY release_date ASC, id) AS duration_sec_change,
        RANK() OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY release_date ASC, id) AS release_date_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_duration_sec,
        LAG(duration_sec, 2, 0) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY release_date ASC, id) AS prev2_duration_sec,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        LAST_VALUE(release_date) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date
    FROM filtered_tracks
    WINDOW w_bpm_tier_by_duration_sec_tie AS (PARTITION BY bpm_tier ORDER BY duration_sec DESC)
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    duration_sec_cume_dist,
    next3_release_date,
    rows_in_is_explicit_artist_id,
    bpm_avg_4,
    is_explicit_artist_id_avg_duration_sec,
    prev2_duration_sec,
    last_known_bpm,
    last_release_date,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
FROM windowed
ORDER BY rows_in_is_explicit_artist_id, release_date DESC NULLS FIRST
