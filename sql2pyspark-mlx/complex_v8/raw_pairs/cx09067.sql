WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        ABS(ms_played - 178909) AS ms_played_distance,
        CASE WHEN device IN ('car', 'speaker') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(ms_played_distance) OVER (PARTITION BY track_id) AS track_id_sum_ms_played_distance,
        SUM(ms_played_distance) OVER (PARTITION BY track_id ORDER BY ms_played_distance, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_distance_sum_2,
        ROUND(CUME_DIST() OVER w_device_group_played_at_to_da, 3) AS ms_played_distance_cume_dist,
        NTILE(10) OVER (PARTITION BY track_id ORDER BY ms_played_distance, id) AS ms_played_distance_decile,
        COUNT(*) OVER w_track_id AS rows_in_track_id,
        NTILE(5) OVER (PARTITION BY device_group, TO_DATE(played_at) ORDER BY ms_played_distance ASC NULLS LAST, id) AS ms_played_distance_quintile
    FROM valid
    WINDOW w_device_group_played_at_to_da AS (PARTITION BY device_group, TO_DATE(played_at) ORDER BY ms_played_distance ASC NULLS LAST), w_track_id AS (PARTITION BY track_id)
),
with_activity AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND UPPER(TRIM(t.title)) LIKE 'R%'
        )
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM with_activity s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played_distance_decile / NULLIF(SUM(ms_played_distance_decile) OVER (PARTITION BY device_group), 0), 4) AS ms_played_distance_decile_share_of_device_group,
        LAG(ms_played_distance_sum_2) OVER (PARTITION BY is_skipped_label, ms_played_distance_quintile ORDER BY ms_played_distance_decile ASC, id) AS prev_ms_played_distance_sum_2,
        COUNT(*) OVER w_device_group AS rows_in_device_group,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device_group)) AS distinct_listener_id_in_device_group,
        NTILE(5) OVER w_all_by_ms_played AS ms_played_quintile
    FROM profiled
    WINDOW w_device_group AS (PARTITION BY device_group), w_all_by_ms_played AS (ORDER BY ms_played DESC, id)
),
with_activity_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 5
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    device,
    ms_played_distance,
    is_skipped_label,
    track_id_sum_ms_played_distance,
    ms_played_distance_sum_2,
    ms_played_distance_cume_dist,
    ms_played_distance_decile,
    rows_in_track_id,
    ms_played_distance_quintile,
    play_count,
    ms_played_distance_decile_share_of_device_group,
    prev_ms_played_distance_sum_2,
    rows_in_device_group,
    distinct_listener_id_in_device_group,
    FLOOR(ms_played_distance / 50000) * 50000 AS ms_played_distance_bucket,
    LOWER(REGEXP_REPLACE(TRIM(is_skipped_label), ' ', '_')) AS is_skipped_label_slug
FROM with_activity_2
WHERE track_id_sum_ms_played_distance BETWEEN 600000 AND 3000000
ORDER BY is_skipped_label_slug, ms_played_distance_decile
