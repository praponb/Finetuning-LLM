WITH base_plays AS (
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
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE source = 'playlist'
      AND device = 'desktop'
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        is_skipped_label,
        CASE WHEN ms_played >= 299017 THEN 'A' WHEN ms_played >= 112786 THEN 'B' ELSE 'C' END AS ms_played_tier,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        TO_DATE(played_at) AS played_at_date
    FROM base_plays
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DATEDIFF(played_at_date, LAG(played_at_date) OVER (PARTITION BY listener_id ORDER BY played_at_plus_1d ASC, id)) AS days_since_prev_played_at_date,
        RANK() OVER w_listener_id_by_played_at_plu AS played_at_plus_1d_rank,
        FIRST_VALUE(played_at_date) OVER (PARTITION BY listener_id ORDER BY played_at_plus_1d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_date,
        FIRST_VALUE(played_at_date) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY played_at_plus_1d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_date
    FROM annotated
    WINDOW w_listener_id_by_played_at_plu AS (PARTITION BY listener_id ORDER BY played_at_plus_1d ASC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY source)) AS distinct_listener_id_in_source,
        ROW_NUMBER() OVER w_all_by_played_at_plus_1d AS rn,
        ROW_NUMBER() OVER (PARTITION BY is_skipped_label ORDER BY played_at_plus_1d ASC, id) AS played_at_plus_1d_seq
    FROM with_trends
    WINDOW w_all_by_played_at_plus_1d AS (ORDER BY played_at_plus_1d DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    is_skipped_label,
    ms_played_tier,
    played_at_date,
    days_since_prev_played_at_date,
    played_at_plus_1d_rank,
    first_played_at_date,
    first_known_played_at_date,
    distinct_listener_id_in_source,
    rn,
    played_at_plus_1d_seq,
    DATE_TRUNC('MONTH', played_at_date) AS played_at_date_month
FROM metrics
WHERE YEAR(first_played_at_date) = 2025
ORDER BY played_at_plus_1d_rank, source DESC
