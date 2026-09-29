WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        CASE WHEN source IN ('radio', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE listener_id IS NOT NULL
),
annotated AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM recent_plays
),
source_group_stats AS (
    -- Max days_since_played_at per source_group
    SELECT source_group, MAX(days_since_played_at) AS max_days_since_played_at
    FROM annotated
    GROUP BY source_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_played_at,
        ROUND(s.days_since_played_at - g.max_days_since_played_at, 2) AS days_since_played_at_minus_max
    FROM annotated s
    JOIN source_group_stats g ON g.source_group = s.source_group
    WHERE s.days_since_played_at = g.max_days_since_played_at
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(max_days_since_played_at) OVER (PARTITION BY is_skipped, track_id ORDER BY days_since_played_at_minus_max, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_max_days_since_played_at,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at, 3) AS played_at_percent_rank
    FROM compared
    WINDOW w_all_by_played_at AS (ORDER BY played_at ASC, id)
)
SELECT
    source_group,
    source,
    SUM(max_days_since_played_at) AS total_max_days_since_played_at,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list
FROM with_trends
GROUP BY GROUPING SETS ((source_group, source), (source_group))
