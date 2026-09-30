WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref_2
    FROM plays
    WHERE track_id IS NOT NULL
      AND device IN ('tv', 'desktop', 'phone')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, TO_DATE(played_at) ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_2,
        DENSE_RANK() OVER w_listener_id_played_at_to_dat_2 AS played_at_dense_rank
    FROM recent_plays
    WINDOW w_listener_id_played_at_to_dat_2 AS (PARTITION BY listener_id, TO_DATE(played_at) ORDER BY played_at ASC)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, played_at_dense_rank, ms_played, last_known_ms_played
    FROM sequenced
) src
UNPIVOT (
    metric_value FOR metric IN (played_at_dense_rank, ms_played, last_known_ms_played)
) unp
ORDER BY id, metric
