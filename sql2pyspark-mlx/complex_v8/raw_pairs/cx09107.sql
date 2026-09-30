WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE source IN ('album', 'radio', 'search')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        ROW_NUMBER() OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id) AS seq_no,
        LEAD(played_at, 3) OVER (ORDER BY ms_played DESC NULLS FIRST, id) AS next3_played_at,
        SUM(ms_played) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7,
        DENSE_RANK() OVER w_listener_id_device_by_ms_pla_3 AS ms_played_dense_rank,
        NTILE(10) OVER w_all_by_ms_played AS ms_played_decile
    FROM prepared
    WINDOW w_listener_id_device_by_ms_pla_3 AS (PARTITION BY listener_id, device ORDER BY ms_played DESC), w_all_by_ms_played AS (ORDER BY ms_played DESC NULLS FIRST, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(next3_played_at) AS next3_played_at_month,
        CASE WHEN seq_no >= 8 THEN 'large' WHEN seq_no >= 5 THEN 'medium' ELSE 'small' END AS seq_no_tier,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM ranked
    WHERE ms_played_dense_rank > 2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT next3_played_at, seq_no_tier, seq_no
    FROM annotated
) src
PIVOT (
    SUM(seq_no)
    FOR seq_no_tier IN ('medium', 'small')
)
