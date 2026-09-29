WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods,
        CASE WHEN bpm >= 143 THEN 'HIGH' WHEN bpm >= 76 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE bpm >= 75
      AND title IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(release_date) OVER (PARTITION BY bpm_tier ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        LEAD(release_date) OVER (PARTITION BY bpm_tier ORDER BY release_date ASC, id) AS next_release_date,
        ROUND(CUME_DIST() OVER (PARTITION BY bpm_tier ORDER BY release_date ASC), 3) AS release_date_cume_dist,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id, bpm_tier ORDER BY release_date NULLS LAST, id)) AS days_since_prev_release_date,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY bpm_tier ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_2,
        ROW_NUMBER() OVER w_bpm_tier_by_release_date AS seq_no,
        DENSE_RANK() OVER (PARTITION BY artist_id, bpm_tier ORDER BY release_date NULLS LAST, id) AS release_date_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY bpm_tier ORDER BY release_date ASC, id), 3) AS release_date_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, bpm_tier ORDER BY release_date NULLS LAST, id), 3) AS release_date_percent_rank
    FROM recent_tracks
    WINDOW w_bpm_tier_by_release_date AS (PARTITION BY bpm_tier ORDER BY release_date ASC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN bpm_tier IN ('HIGH', 'MEDIUM') THEN 'group_a' WHEN bpm_tier = 'LOW' THEN 'group_b' ELSE 'other' END AS bpm_tier_group,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        MONTH(release_date) AS release_date_month
    FROM with_trends
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(release_date_month / NULLIF(SUM(release_date_month) OVER (PARTITION BY bpm_tier), 0), 4) AS release_date_month_share_of_bpm_tier,
        ROUND(AVG(days_since_prev_release_date) OVER (PARTITION BY bpm_tier), 2) AS bpm_tier_avg_days_since_prev_release_date,
        ROW_NUMBER() OVER (PARTITION BY bpm_tier_group ORDER BY release_date_month ASC, id) AS seq_no_2,
        MAX(release_date_percent_rank) OVER (PARTITION BY bpm_tier_group ORDER BY release_date_month ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS release_date_percent_rank_max_1_prior
    FROM enriched
)
SELECT
    id,
    artist_id,
    title,
    moods,
    bpm_tier,
    first_release_date,
    last_known_release_date,
    next_release_date,
    days_since_prev_release_date,
    last_known_release_date_2,
    release_date_cume_dist_2,
    bpm_tier_group,
    release_date_label,
    release_date_month,
    release_date_month_share_of_bpm_tier,
    bpm_tier_avg_days_since_prev_release_date,
    seq_no_2
FROM ranked
WHERE release_date_cume_dist <= 0.8
ORDER BY title DESC, last_known_release_date_2
