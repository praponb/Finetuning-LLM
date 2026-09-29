WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE artist_id IS NOT NULL
),
joined_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.genre AS artist_genre, a.is_verified AS artist_is_verified
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(release_date, 2) OVER (PARTITION BY artist_genre ORDER BY bpm DESC, id) AS prev2_release_date,
        MIN(bpm_bucket) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_bucket_so_far,
        NTILE(4) OVER (PARTITION BY artist_genre ORDER BY bpm DESC, id) AS bpm_quartile,
        MAX(bpm_bucket) OVER (PARTITION BY artist_is_verified ORDER BY release_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_bucket_max_4,
        NTILE(4) OVER w_artist_id_by_bpm AS bpm_quartile_2,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        LEAD(release_date) OVER (PARTITION BY artist_is_verified ORDER BY release_date, id) AS next_release_date,
        bpm_bucket - LAG(bpm_bucket) OVER w_artist_is_verified_by_releas_2 AS bpm_bucket_change,
        RANK() OVER (PARTITION BY artist_id ORDER BY bpm, id) AS bpm_rank,
        ROUND(bpm_bucket / NULLIF(SUM(bpm_bucket) OVER (PARTITION BY artist_is_verified), 0), 4) AS bpm_bucket_share_of_artist_is_verified,
        LAST_VALUE(bpm_bucket) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_bucket
    FROM joined_artist
    WINDOW w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm, id), w_artist_is_verified_by_releas_2 AS (PARTITION BY artist_is_verified ORDER BY release_date, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    moods,
    title_prefix,
    bpm_bucket,
    artist_genre,
    prev2_release_date,
    min_bpm_bucket_so_far,
    bpm_bucket_max_4,
    bpm_quartile_2,
    last_release_date,
    next_release_date,
    bpm_bucket_change,
    bpm_bucket_share_of_artist_is_verified,
    last_known_bpm_bucket,
    COALESCE(bpm_bucket, 0) AS bpm_bucket_filled
FROM with_trends
ORDER BY bpm_bucket_change, bpm_bucket_share_of_artist_is_verified DESC NULLS FIRST
