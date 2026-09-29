WITH valid AS (
    -- Filter plays
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
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_source_by_ms_played_ties, 3) AS ms_played_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY source_or_unknown, track_id), 0), 4) AS ms_played_share_of_source_or_unknown_track_id,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        NTILE(4) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played ASC, id) AS ms_played_quartile,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER w_source_or_unknown_track_id) AS distinct_listener_id_in_source_or_unknown_track_,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY source_or_unknown, track_id)) AS distinct_source_in_source_or_unknown_track_id,
        DENSE_RANK() OVER (ORDER BY ms_played DESC, id) AS ms_played_dense_rank
    FROM valid
    WINDOW w_source_by_ms_played_ties AS (PARTITION BY source ORDER BY ms_played DESC), w_source_or_unknown_track_id AS (PARTITION BY source_or_unknown, track_id)
),
annotated AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(source)) AS source_len
    FROM sequenced
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id), 2) AS ms_played_vs_track_id_avg,
        LEAD(ms_played_dense_rank, 3) OVER (ORDER BY ms_played_quartile DESC, id) AS next3_ms_played_dense_rank,
        SUM(ms_played) OVER (PARTITION BY track_id) AS track_id_sum_ms_played,
        ROUND(CUME_DIST() OVER (ORDER BY distinct_listener_id_in_source_or_unknown_track_, id), 3) AS distinct_listener_id_in_source_or_unknown_track__2
    FROM annotated
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    source_or_unknown,
    ms_played_percent_rank,
    first_ms_played,
    ms_played_quartile,
    distinct_listener_id_in_source_or_unknown_track_,
    source_len,
    ms_played_vs_track_id_avg,
    next3_ms_played_dense_rank,
    distinct_listener_id_in_source_or_unknown_track__2,
    CASE WHEN ms_played_dense_rank >= 8 THEN 'large' WHEN ms_played_dense_rank >= 4 THEN 'medium' ELSE 'small' END AS ms_played_dense_rank_tier,
    LOWER(REGEXP_REPLACE(TRIM(source_or_unknown), ' ', '_')) AS source_or_unknown_slug
FROM windowed
WHERE first_ms_played > 108979
ORDER BY first_ms_played DESC, id
LIMIT 10
