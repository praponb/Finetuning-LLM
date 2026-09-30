WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        CASE WHEN bpm >= 142 THEN 'A' WHEN bpm >= 94 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM tracks
    WHERE (release_date < DATE '2025-03-29' OR artist_id IS NOT NULL)
),
deduped AS (
    -- Keep the latest row per bpm_tier, artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY bpm_tier, artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
deduped_2 AS (
    -- Keep the latest row per bpm_tier
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY bpm_tier ORDER BY duration_sec DESC, id DESC) AS recency_rank
        FROM deduped
    ) ranked
    WHERE recency_rank = 1
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM deduped_2 s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = FALSE
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_bpm_tier_artist_id_by_releas, 3) AS release_date_cume_dist,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_1_prior,
        SIZE(COLLECT_SET(bpm_tier) OVER w_is_explicit_artist_id) AS distinct_bpm_tier_in_is_explicit_artist_id,
        MAX(duration_sec) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_duration_sec,
        RANK() OVER w_all_by_release_date AS release_date_rank
    FROM screened
    WINDOW w_bpm_tier_artist_id_by_releas AS (PARTITION BY bpm_tier, artist_id ORDER BY release_date, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_all_by_release_date AS (ORDER BY release_date ASC, id)
)
SELECT
    distinct_bpm_tier_in_is_explicit_artist_id,
    bpm_tier,
    release_date_rank,
    GROUPING(distinct_bpm_tier_in_is_explicit_artist_id) AS is_total_distinct_bpm_tier_in_is_explicit_artist,
    MIN(distinct_bpm_tier_in_is_explicit_artist_id) AS min_distinct_bpm_tier_in_is_explicit_artist_id
FROM with_trends
GROUP BY GROUPING SETS ((distinct_bpm_tier_in_is_explicit_artist_id, bpm_tier, release_date_rank), (distinct_bpm_tier_in_is_explicit_artist_id), (bpm_tier), ())
ORDER BY distinct_bpm_tier_in_is_explicit_artist_id NULLS LAST, bpm_tier NULLS LAST, release_date_rank NULLS LAST
