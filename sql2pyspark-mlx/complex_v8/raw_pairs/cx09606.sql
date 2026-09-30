WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        CASE WHEN bpm >= 143 THEN 'large' WHEN bpm >= 85 THEN 'medium' ELSE 'small' END AS bpm_tier
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND NOT is_explicit
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_artist_id_bpm_tier_by_releas AS release_date_dense_rank,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY YEAR(release_date), bpm_tier ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_id, bpm_tier ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(release_date), bpm_tier ORDER BY bpm ASC), 3) AS bpm_percent_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY bpm_tier), 0), 4) AS duration_sec_share_of_bpm_tier,
        MAX(bpm) OVER (PARTITION BY bpm_tier) AS bpm_tier_max_bpm
    FROM recent_tracks
    WINDOW w_artist_id_bpm_tier_by_releas AS (PARTITION BY artist_id, bpm_tier ORDER BY release_date DESC NULLS FIRST)
),
latest AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        bpm_tier,
        release_date_dense_rank,
        second_bpm,
        last_duration_sec,
        bpm_percent_rank,
        duration_sec_share_of_bpm_tier,
        bpm_tier_max_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date, id) AS recency_rank
        FROM with_windows
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, bpm_tier_max_bpm, second_bpm, last_duration_sec, duration_sec
    FROM latest
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (bpm_tier_max_bpm, second_bpm, last_duration_sec, duration_sec)
) unp
ORDER BY id, field
