WITH valid AS (
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
        CASE WHEN duration_sec >= 352 THEN 'HIGH' WHEN duration_sec >= 165 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
    FROM tracks
    WHERE artist_id IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, a.artist_name
    FROM valid s
    LEFT JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_duration_sec_tier_artist_id, 0), 4) AS bpm_share_of_duration_sec_tier_artist_id,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY duration_sec_tier ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        COUNT(*) OVER (PARTITION BY duration_sec_tier, artist_id) AS rows_in_duration_sec_tier_artist_id,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm ASC, id)) AS days_since_prev_release_date,
        SIZE(COLLECT_SET(duration_sec_tier) OVER (PARTITION BY is_explicit)) AS distinct_duration_sec_tier_in_is_explicit,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY duration_sec_tier, artist_id), 0), 4) AS duration_sec_share_of_duration_sec_tier_artist_i,
        NTILE(10) OVER (PARTITION BY is_explicit ORDER BY release_date, id) AS release_date_decile,
        LEAD(release_date, 3) OVER w_is_explicit_by_duration_sec AS next3_release_date,
        LEAD(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC, id) AS next_duration_sec,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY duration_sec_tier ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date
    FROM joined
    WINDOW w_duration_sec_tier_artist_id AS (PARTITION BY duration_sec_tier, artist_id), w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec DESC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        duration_sec_tier,
        artist_name,
        bpm_share_of_duration_sec_tier_artist_id,
        second_bpm,
        bpm_share_of_is_explicit,
        days_since_prev_release_date,
        distinct_duration_sec_tier_in_is_explicit,
        duration_sec_share_of_duration_sec_tier_artist_i,
        release_date_decile,
        next_duration_sec,
        last_known_release_date,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        ROUND(bpm_share_of_is_explicit * 1.5, 2) AS bpm_share_of_is_explicit_adj
    FROM metrics
)
SELECT
    id,
    artist_id,
    release_date,
    artist_name,
    second_bpm,
    bpm_share_of_is_explicit,
    days_since_prev_release_date,
    distinct_duration_sec_tier_in_is_explicit,
    duration_sec_share_of_duration_sec_tier_artist_i,
    release_date_decile,
    next_duration_sec,
    last_known_release_date,
    duration_sec_bucket,
    bpm_share_of_is_explicit_adj,
    ROUND(bpm_share_of_is_explicit * 0.9, 2) AS bpm_share_of_is_explicit_adj_2
FROM derived
ORDER BY bpm_share_of_is_explicit_adj_2 NULLS LAST, id
