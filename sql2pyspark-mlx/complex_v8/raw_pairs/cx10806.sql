WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source <> 'radio'
      AND played_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        ROUND(PERCENT_RANK() OVER w_is_skipped_device_by_played, 3) AS played_at_percent_rank
    FROM scoped_plays
    WINDOW w_is_skipped_device_by_played AS (PARTITION BY is_skipped, device ORDER BY played_at ASC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        SUM(played_at_percent_rank) OVER w_device AS device_sum_played_at_percent_rank,
        ROUND(played_at_percent_rank / NULLIF(SUM(played_at_percent_rank) OVER (PARTITION BY device), 0), 4) AS played_at_percent_rank_share_of_device,
        SUM(played_at_percent_rank) OVER (ORDER BY played_at_percent_rank DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS played_at_percent_rank_sum_2,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device
    FROM with_windows
    WINDOW w_device AS (PARTITION BY device)
)
SELECT id, field, metric_value
FROM (
    SELECT id, device_sum_played_at_percent_rank, played_at_percent_rank_share_of_device
    FROM metrics
) src
UNPIVOT (
    metric_value FOR field IN (device_sum_played_at_percent_rank AS m_device_sum_played_at_percent_rank, played_at_percent_rank_share_of_device AS played_at_percent_rank_share_of_device_v)
) unp
