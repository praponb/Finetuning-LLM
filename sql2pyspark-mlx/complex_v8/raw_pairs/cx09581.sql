WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE (ms_played > 51883 OR track_id IS NOT NULL)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, device ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, device ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played
    FROM valid_plays
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(ms_played) OVER w_device_is_skipped_by_ms_play AS prev_ms_played,
        MIN(last_known_ms_played) OVER (PARTITION BY device, listener_id) AS device_listener_id_min_last_known_ms_played,
        RANK() OVER (PARTITION BY device, is_skipped ORDER BY ms_played_avg_6_prior ASC, id) AS ms_played_avg_6_prior_rank,
        ROUND(PERCENT_RANK() OVER w_device_is_skipped_by_ms_play, 3) AS ms_played_avg_6_prior_percent_rank
    FROM sequenced
    WINDOW w_device_is_skipped_by_ms_play AS (PARTITION BY device, is_skipped ORDER BY ms_played_avg_6_prior ASC, id)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM metrics s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
per_group AS (
    -- Summarise per group
    SELECT
        listener_id,
        ms_played_avg_6_prior_rank,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        MAX(prev_ms_played) AS max_prev_ms_played,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list
    FROM with_stats
    GROUP BY listener_id, ms_played_avg_6_prior_rank
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ARRAY_JOIN(ARRAY_SORT(device_list), ',') AS device_list_text,
        CASE WHEN distinct_listener_ids >= 6 THEN 'HIGH' WHEN distinct_listener_ids >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_listener_ids_tier,
        CASE WHEN distinct_listener_ids >= 7 THEN 'HIGH' WHEN distinct_listener_ids >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_listener_ids_tier_2
    FROM per_group
    WHERE ms_played_avg_6_prior_rank > 5
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.listener_id,
        s.ms_played_avg_6_prior_rank,
        s.distinct_listener_ids,
        s.max_prev_ms_played,
        s.device_list,
        s.device_list_text,
        s.distinct_listener_ids_tier,
        s.distinct_listener_ids_tier_2,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM enriched s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    listener_id,
    ms_played_avg_6_prior_rank,
    max_prev_ms_played,
    device_list_text,
    distinct_listener_ids_tier,
    distinct_listener_ids_tier_2,
    listener_plan,
    listener_interests
FROM with_parent_listener
ORDER BY distinct_listener_ids_tier DESC NULLS FIRST, ms_played_avg_6_prior_rank NULLS LAST
