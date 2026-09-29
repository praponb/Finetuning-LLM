WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec BETWEEN 144 AND 409
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS recency_rank
        FROM prepared_tracks
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id), 3) AS bpm_cume_dist,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC, id) AS bpm_change,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC, id), 3) AS bpm_cume_dist_2,
        RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC) AS bpm_rank,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id)) AS days_since_prev_release_date
    FROM unique_rows
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    moods,
    bpm_cume_dist,
    bpm_cume_dist_2,
    days_since_prev_release_date
FROM with_trends
