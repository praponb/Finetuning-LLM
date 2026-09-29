WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM plays
    WHERE track_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped), 2) AS is_skipped_avg_ms_played,
        SUM(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_sum_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_is_skipped, 0), 4) AS ms_played_share_of_is_skipped
    FROM recent
    WINDOW w_is_skipped AS (PARTITION BY is_skipped)
),
latest AS (
    -- Keep the latest row per track_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        source_prefix,
        device_prefix,
        is_skipped_avg_ms_played,
        is_skipped_sum_ms_played,
        last_known_ms_played,
        second_ms_played,
        ms_played_share_of_is_skipped
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY is_skipped_avg_ms_played DESC, id DESC) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.ms_played >= 51883
              AND p.played_at >= TIMESTAMP '2025-01-31 12:00:00'
        )
      AND s.last_known_ms_played >= 160264
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    source,
    source_prefix,
    device_prefix,
    is_skipped_avg_ms_played,
    second_ms_played,
    ms_played_share_of_is_skipped,
    COALESCE(second_ms_played, 0) AS second_ms_played_filled
FROM without_matches
ORDER BY id DESC, source_prefix
