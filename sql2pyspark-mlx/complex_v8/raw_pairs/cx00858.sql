WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN ms_played >= 308104 THEN 'A' WHEN ms_played >= 73625 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY MONTH(played_at)), 2) AS played_at_month_avg_ms_played,
        LEAD(played_at) OVER (ORDER BY ms_played, id) AS next_played_at,
        SIZE(COLLECT_SET(ms_played_tier) OVER (PARTITION BY MONTH(played_at))) AS distinct_ms_played_tier_in_played_at_month,
        COUNT(*) OVER w_played_at_month AS rows_in_played_at_month,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(PERCENT_RANK() OVER w_source_by_played_at_ties, 3) AS played_at_percent_rank,
        ROUND(CUME_DIST() OVER w_source_by_played_at, 3) AS played_at_cume_dist,
        LAG(ms_played) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_ms_played,
        SUM(ms_played) OVER (PARTITION BY MONTH(played_at)) AS played_at_month_sum_ms_played
    FROM valid_plays
    WINDOW w_played_at_month AS (PARTITION BY MONTH(played_at)), w_source_by_played_at_ties AS (PARTITION BY source ORDER BY played_at), w_source_by_played_at AS (PARTITION BY source ORDER BY played_at, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        ms_played_tier,
        played_at_month_avg_ms_played,
        next_played_at,
        distinct_ms_played_tier_in_played_at_month,
        rows_in_played_at_month,
        second_played_at,
        played_at_percent_rank,
        played_at_cume_dist,
        prev_ms_played,
        played_at_month_sum_ms_played,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM sequenced
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prev_ms_played / NULLIF(SUM(prev_ms_played) OVER (PARTITION BY distinct_ms_played_tier_in_played_at_month, listener_id), 0), 4) AS prev_ms_played_share_of_distinct_ms_played_tier_,
        SIZE(COLLECT_SET(device) OVER w_distinct_ms_played_tier_in_p) AS distinct_device_in_distinct_ms_played_tier_in_pl,
        FIRST_VALUE(next_played_at) OVER (ORDER BY next_played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_played_at,
        COUNT(*) OVER (PARTITION BY MONTH(second_played_at) ORDER BY second_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(rows_in_played_at_month) OVER (PARTITION BY distinct_ms_played_tier_in_played_at_month ORDER BY ms_played_filled DESC, id) AS next_rows_in_played_at_month
    FROM derived
    WINDOW w_distinct_ms_played_tier_in_p AS (PARTITION BY distinct_ms_played_tier_in_played_at_month, listener_id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played_tier,
    next_played_at,
    rows_in_played_at_month,
    played_at_percent_rank,
    played_at_cume_dist,
    prev_ms_played,
    played_at_month_sum_ms_played,
    ms_played_filled,
    first_next_played_at,
    running_count,
    next_rows_in_played_at_month,
    next_played_at + INTERVAL 15 MINUTES AS next_played_at_plus_15m
FROM sequenced_2
WHERE distinct_ms_played_tier_in_played_at_month <= 8
ORDER BY listener_id
