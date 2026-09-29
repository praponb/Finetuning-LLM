WITH scoped_plays AS (
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
        TO_DATE(played_at) AS played_at_date,
        CASE WHEN ms_played >= 218129 THEN 'HIGH' WHEN ms_played >= 73625 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE ms_played <= 318978
      AND NOT is_skipped
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(played_at) OVER (PARTITION BY device, is_skipped ORDER BY played_at ASC, id) AS next_played_at,
        COUNT(*) OVER (PARTITION BY device, is_skipped ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY device, is_skipped ORDER BY played_at ASC, id), 3) AS played_at_cume_dist,
        LAST_VALUE(played_at) OVER (PARTITION BY device, is_skipped ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        RANK() OVER (PARTITION BY device, is_skipped ORDER BY played_at ASC) AS played_at_rank
    FROM scoped_plays
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_date,
        ms_played_tier,
        source_len,
        next_played_at,
        running_count,
        played_at_cume_dist,
        last_played_at
    FROM with_windows
    WHERE played_at_rank <= 3
      AND played_at_cume_dist < 0.7
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, ms_played_tier, source_len
    FROM first_rows
) src
PIVOT (
    MIN(source_len)
    FOR ms_played_tier IN ('LOW', 'HIGH')
)
ORDER BY track_id
