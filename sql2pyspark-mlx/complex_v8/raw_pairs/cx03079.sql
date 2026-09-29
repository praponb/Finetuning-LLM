WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE source = 'radio'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY played_at_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        NTH_VALUE(played_at_date, 2) OVER (PARTITION BY is_skipped ORDER BY played_at_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_date,
        RANK() OVER (PARTITION BY is_skipped ORDER BY played_at_date DESC NULLS FIRST) AS played_at_date_rank,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_filled_share_of_listener_id,
        RANK() OVER (PARTITION BY is_skipped ORDER BY played_at_date DESC NULLS FIRST, id) AS played_at_date_rank_2,
        LAG(ms_played_filled, 1, 0) OVER (PARTITION BY is_skipped ORDER BY played_at_date DESC NULLS FIRST, id) AS prev_ms_played_filled
    FROM base
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        device,
        source,
        played_at_date,
        ms_played_filled,
        first_known_ms_played,
        played_at_date_rank,
        ms_played_filled_share_of_listener_id,
        played_at_date_rank_2,
        ABS(ms_played_bucket - 123500) AS ms_played_bucket_distance
    FROM with_windows
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        device,
        ms_played_filled,
        first_known_ms_played,
        played_at_date_rank,
        ms_played_filled_share_of_listener_id,
        played_at_date_rank_2,
        ms_played_bucket_distance,
        ROUND(first_known_ms_played / NULLIF(played_at_date_rank_2, 0), 3) AS first_known_ms_played_per_played_at_date_rank_2,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM annotated
)
SELECT *
FROM (
    SELECT played_at_date_rank, device, source_or_unknown, first_known_ms_played_per_played_at_date_rank_2
    FROM enriched
) src
PIVOT (
    MAX(first_known_ms_played_per_played_at_date_rank_2)
    FOR source_or_unknown IN ('album', 'UNKNOWN', 'radio')
)
ORDER BY played_at_date_rank, device
