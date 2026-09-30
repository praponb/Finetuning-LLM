WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        CONCAT_WS(' / ', device, source) AS device_source_label
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-06'
      AND track_id >= 26
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(device_source_label, '#', CAST(ms_played_filled AS STRING)) AS device_source_label_ref
    FROM scoped_plays
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        device,
        source,
        source_prefix,
        device_source_label,
        device_source_label_ref,
        ROUND(AVG(ms_played_filled) OVER w_played_at_month, 2) AS played_at_month_avg_ms_played_filled,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at)), 2) AS ms_played_filled_vs_played_at_month_avg,
        RANK() OVER (PARTITION BY device, listener_id ORDER BY ms_played_filled DESC, id) AS ms_played_filled_rank,
        FIRST_VALUE(ms_played_filled) OVER (PARTITION BY device, listener_id ORDER BY ms_played_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY source, track_id ORDER BY ms_played_filled ASC, id), 3) AS ms_played_filled_cume_dist
    FROM annotated
    WINDOW w_played_at_month AS (PARTITION BY DATE_TRUNC('MONTH', played_at))
)
SELECT *
FROM (
    SELECT source, device, played_at_month_avg_ms_played_filled
    FROM windowed
) src
PIVOT (
    MIN(played_at_month_avg_ms_played_filled) AS min_v, SUM(played_at_month_avg_ms_played_filled) AS sum_v
    FOR device IN ('tv', 'car', 'speaker', 'desktop')
)
