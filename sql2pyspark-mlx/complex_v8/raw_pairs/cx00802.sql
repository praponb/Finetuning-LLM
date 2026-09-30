WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        CASE WHEN ms_played >= 225733 THEN 'HIGH' WHEN ms_played >= 107912 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        CASE WHEN ms_played >= 308104 THEN 'large' WHEN ms_played >= 151112 THEN 'medium' ELSE 'small' END AS ms_played_tier_2,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE track_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM filtered
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        LAST_VALUE(ms_played) OVER (PARTITION BY track_id, listener_id ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY is_skipped), 0), 4) AS ms_played_share_of_is_skipped,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, listener_id ORDER BY ms_played NULLS LAST), 3) AS ms_played_cume_dist,
        DENSE_RANK() OVER w_track_id_listener_id_by_ms_p_3 AS ms_played_dense_rank
    FROM annotated
    WINDOW w_track_id_listener_id_by_ms_p_3 AS (PARTITION BY track_id, listener_id ORDER BY ms_played NULLS LAST, id)
)
SELECT
    is_skipped,
    listener_id,
    COALESCE(source, 'ALL') AS source,
    GROUPING_ID() AS grouping_level,
    SUM(ms_played_share_of_is_skipped) AS total_ms_played_share_of_is_skipped
FROM metrics
GROUP BY GROUPING SETS ((is_skipped, listener_id, source), (is_skipped))
ORDER BY is_skipped NULLS LAST, listener_id NULLS LAST, source NULLS LAST
