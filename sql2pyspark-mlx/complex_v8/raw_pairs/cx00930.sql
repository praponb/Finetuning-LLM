WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 09:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(ms_played) OVER (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        MIN(ms_played) OVER w_listener_id AS listener_id_min_ms_played,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        COUNT(*) OVER w_listener_id AS rows_in_listener_id,
        LAG(ms_played, 2, 0) OVER (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id) AS prev2_ms_played,
        SUM(ms_played) OVER (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_sum_2
    FROM valid_plays
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN device IN ('car', 'phone') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        GREATEST(ms_played_sum_2, rows_in_listener_id) AS greatest_ms_played_sum_2_rows_in_listener_id
    FROM windowed
),
summary AS (
    -- Summarise per group
    SELECT
        source,
        MAX(rows_in_listener_id) AS max_rows_in_listener_id,
        SUM(ms_played_sum_2) AS total_ms_played_sum_2,
        ROUND(AVG(running_ms_played), 2) AS avg_running_ms_played,
        ROUND(SUM(listener_id_min_ms_played) / NULLIF(SUM(second_ms_played), 0), 3) AS listener_id_min_ms_played_to_second_ms_played_ra,
        SUM(ms_played_sum_2) FILTER (WHERE device NOT IN ('phone', 'speaker')) AS ms_played_sum_2_device_not_in_phone_speaker
    FROM annotated
    GROUP BY source
    HAVING MAX(ms_played) > 56858
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        source,
        max_rows_in_listener_id,
        total_ms_played_sum_2,
        avg_running_ms_played,
        listener_id_min_ms_played_to_second_ms_played_ra,
        ms_played_sum_2_device_not_in_phone_speaker,
        ROUND(AVG(max_rows_in_listener_id) OVER (PARTITION BY source), 2) AS source_avg_max_rows_in_listener_id,
        MIN(total_ms_played_sum_2) OVER w_source AS source_min_total_ms_played_sum_2
    FROM summary
    WINDOW w_source AS (PARTITION BY source)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        avg_running_ms_played - LAG(avg_running_ms_played) OVER (ORDER BY max_rows_in_listener_id NULLS LAST, source) AS avg_running_ms_played_change,
        LAST_VALUE(total_ms_played_sum_2) IGNORE NULLS OVER (ORDER BY max_rows_in_listener_id NULLS LAST, source ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_ms_played_sum_2,
        ROW_NUMBER() OVER w_all_by_max_rows_in_listener AS rn
    FROM sequenced
    WINDOW w_all_by_max_rows_in_listener AS (ORDER BY max_rows_in_listener_id NULLS LAST, source)
)
SELECT
    source,
    max_rows_in_listener_id,
    total_ms_played_sum_2,
    avg_running_ms_played,
    listener_id_min_ms_played_to_second_ms_played_ra,
    source_avg_max_rows_in_listener_id,
    last_known_total_ms_played_sum_2,
    rn,
    ROUND(avg_running_ms_played * 1.1, 2) AS avg_running_ms_played_adj,
    GREATEST(avg_running_ms_played, total_ms_played_sum_2) AS greatest_avg_running_ms_played_total_ms_played_s
FROM sequenced_2
WHERE rn BETWEEN 4 AND 9
ORDER BY listener_id_min_ms_played_to_second_ms_played_ra DESC
