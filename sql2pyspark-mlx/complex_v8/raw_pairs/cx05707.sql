WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 09:00:00'
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at < TIMESTAMP '2025-03-08 10:00:00'
              AND p.ms_played <= 282311
        )
      AND s.listener_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        source_ref,
        prev_played_at,
        session_no,
        ROW_NUMBER() OVER w_track_id_device_by_played_at AS rn,
        RANK() OVER (PARTITION BY track_id, device ORDER BY played_at ASC) AS played_at_rank,
        LAG(ms_played) OVER (PARTITION BY track_id, device ORDER BY played_at ASC, id) AS prev_ms_played,
        RANK() OVER (PARTITION BY track_id, device ORDER BY played_at ASC, id) AS played_at_rank_2,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id, device ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(AVG(is_new_session) OVER (PARTITION BY track_id, device ORDER BY played_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS is_new_session_avg_2_prior
    FROM qualified
    WINDOW w_track_id_device_by_played_at AS (PARTITION BY track_id, device ORDER BY played_at ASC, id)
)
SELECT id, metric, metric_value
FROM (
    SELECT id, ms_played, rn
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric IN (ms_played, rn)
) unp
