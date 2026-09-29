WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN duration_sec >= 365 THEN 'HIGH' WHEN duration_sec >= 165 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
    FROM tracks
    WHERE LOWER(title) LIKE '%lo%'
      AND duration_sec < 353
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit_duration_sec_tie) AS distinct_artist_id_in_is_explicit_duration_sec_t,
        COUNT(*) OVER (PARTITION BY is_explicit, duration_sec_tier) AS rows_in_is_explicit_duration_sec_tier,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit, duration_sec_tier), 0), 4) AS duration_sec_share_of_is_explicit_duration_sec_t,
        MAX(duration_sec) OVER (PARTITION BY is_explicit, duration_sec_tier) AS is_explicit_duration_sec_tier_max_duration_sec,
        FIRST_VALUE(bpm) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        COUNT(*) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_dense_rank,
        MIN(bpm) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        ROW_NUMBER() OVER w_all_by_duration_sec AS seq_no,
        MAX(bpm) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_max_3,
        NTILE(4) OVER (ORDER BY duration_sec DESC, id) AS duration_sec_quartile
    FROM filtered_tracks
    WINDOW w_is_explicit_duration_sec_tie AS (PARTITION BY is_explicit, duration_sec_tier), w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_tier,
        last_bpm,
        last_known_release_date,
        distinct_artist_id_in_is_explicit_duration_sec_t,
        rows_in_is_explicit_duration_sec_tier,
        duration_sec_share_of_is_explicit_duration_sec_t,
        is_explicit_duration_sec_tier_max_duration_sec,
        first_bpm,
        running_count,
        min_bpm_so_far,
        seq_no,
        bpm_max_3,
        duration_sec_quartile
    FROM metrics
    WHERE duration_sec_dense_rank = 1
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    duration_sec_tier,
    last_bpm,
    distinct_artist_id_in_is_explicit_duration_sec_t,
    rows_in_is_explicit_duration_sec_tier,
    duration_sec_share_of_is_explicit_duration_sec_t,
    is_explicit_duration_sec_tier_max_duration_sec,
    running_count,
    min_bpm_so_far,
    seq_no,
    bpm_max_3
FROM top_rows
ORDER BY id, duration_sec_tier NULLS LAST
LIMIT 20
