WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at
    FROM base_plays s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY source ORDER BY ms_played), 3) AS ms_played_cume_dist,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY source, is_skipped), 0), 4) AS ms_played_share_of_source_is_skipped
    FROM summarized_rows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, ms_played_share_of_source_is_skipped, ms_played_cume_dist
    FROM metrics
) src
UNPIVOT (
    amount FOR metric IN (ms_played_share_of_source_is_skipped, ms_played_cume_dist)
) unp
ORDER BY id, metric
