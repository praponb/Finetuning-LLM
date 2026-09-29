WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE NOT is_skipped
      AND track_id >= 19
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        l.birth_year AS listener_birth_year,
        t.moods AS track_moods
    FROM prepared s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(device)) AS device_len,
        LENGTH(TRIM(device)) AS device_len_2
    FROM with_parent_listener
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(device_len_2 - AVG(device_len_2) OVER (PARTITION BY device_len), 2) AS device_len_2_vs_device_len_avg,
        LEAD(device_len, 3) OVER (PARTITION BY listener_id, track_id ORDER BY device_len_2 DESC, id) AS next3_device_len,
        ROUND(AVG(device_len) OVER (PARTITION BY listener_id, track_id ORDER BY device_len_2 DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS device_len_avg_3,
        FIRST_VALUE(listener_birth_year) IGNORE NULLS OVER (PARTITION BY listener_id, track_id ORDER BY device_len_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_listener_birth_year,
        COUNT(*) OVER w_device_len AS rows_in_device_len,
        LAG(device_len_2) OVER w_listener_id_track_id_by_devi AS prev_device_len_2
    FROM scored
    WINDOW w_device_len AS (PARTITION BY device_len), w_listener_id_track_id_by_devi AS (PARTITION BY listener_id, track_id ORDER BY device_len_2 DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        device_len,
        device_len_2_vs_device_len_avg,
        next3_device_len,
        rows_in_device_len,
        prev_device_len_2,
        COUNT(*) OVER (PARTITION BY listener_id, device_len_2) AS rows_in_listener_id_device_len_2,
        rows_in_device_len - LAG(rows_in_device_len) OVER w_all_by_listener_birth_year AS rows_in_device_len_change,
        next3_device_len - LAG(next3_device_len) OVER w_all_by_listener_birth_year AS next3_device_len_change
    FROM sequenced
    WINDOW w_all_by_listener_birth_year AS (ORDER BY listener_birth_year DESC, id)
)
SELECT
    id,
    listener_id,
    device,
    device_len,
    device_len_2_vs_device_len_avg,
    rows_in_device_len,
    prev_device_len_2,
    rows_in_listener_id_device_len_2,
    next3_device_len_change,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown,
    CASE WHEN device IN ('desktop', 'phone') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
FROM metrics
ORDER BY next3_device_len_change, device_group DESC, id
LIMIT 20
