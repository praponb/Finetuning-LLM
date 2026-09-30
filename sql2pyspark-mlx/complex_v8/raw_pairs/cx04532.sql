WITH filtered_plays AS (
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
        CASE WHEN ms_played >= 301865 THEN 'large' WHEN ms_played >= 71476 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 09:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        SUM(ms_played) OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        NTILE(4) OVER w_all_by_played_at AS played_at_quartile,
        ROW_NUMBER() OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC, id) AS played_at_seq,
        LEAD(played_at, 3) OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC, id) AS next3_played_at,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        MAX(ms_played) OVER (PARTITION BY ms_played_tier) AS ms_played_tier_max_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY ms_played_tier), 2) AS ms_played_vs_ms_played_tier_avg,
        ROW_NUMBER() OVER (ORDER BY played_at, id) AS rn,
        LAG(ms_played, 2) OVER (ORDER BY played_at, id) AS prev2_ms_played,
        NTILE(4) OVER w_ms_played_tier_by_played_at AS played_at_quartile_2,
        ms_played - LAG(ms_played) OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC, id) AS ms_played_change,
        RANK() OVER (PARTITION BY ms_played_tier ORDER BY played_at NULLS LAST) AS played_at_rank
    FROM filtered_plays
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id), w_ms_played_tier AS (PARTITION BY ms_played_tier), w_ms_played_tier_by_played_at AS (PARTITION BY ms_played_tier ORDER BY played_at NULLS LAST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM windowed
    WHERE played_at_quartile_2 < 9
)
-- Final projection
SELECT
    id,
    track_id,
    device,
    is_skipped,
    source,
    running_ms_played,
    played_at_quartile,
    next3_played_at,
    second_ms_played,
    ms_played_tier_max_ms_played,
    ms_played_change,
    played_at_rank,
    source_or_unknown,
    INITCAP(TRIM(source_or_unknown)) AS source_or_unknown_clean
FROM enriched
