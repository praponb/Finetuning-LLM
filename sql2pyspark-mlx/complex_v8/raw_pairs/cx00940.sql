WITH recent AS (
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
        CASE WHEN ms_played >= 302279 THEN 'large' WHEN ms_played >= 73625 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        device,
        DATE_TRUNC('MONTH', played_at_year) AS played_at_year_month,
        SUM(ms_played) FILTER (WHERE device IS NOT NULL) AS ms_played_device_is_not_null,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        SUM(ms_played) AS total_ms_played,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        MIN(played_at_year) AS first_played_at_year
    FROM recent
    WHERE ms_played >= 71476
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE total_ms_played < (SELECT MAX(total_ms_played) FROM grouped)
),
qualified_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM qualified
    WHERE total_ms_played > (SELECT MIN(total_ms_played) FROM qualified)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(total_ms_played, 3) OVER w_all_by_total_ms_played AS prev3_total_ms_played,
        ROUND(AVG(distinct_listener_ids) OVER w_first_played_at_year_to_date, 2) AS first_played_at_year_to_date_source_avg_distinct
    FROM qualified_2
    WINDOW w_all_by_total_ms_played AS (ORDER BY total_ms_played NULLS LAST, source, device, played_at_year_month), w_first_played_at_year_to_date AS (PARTITION BY TO_DATE(first_played_at_year), source)
)
SELECT source, device, metric_name, value
FROM (
    SELECT source, device, ms_played_device_is_not_null, distinct_listener_ids, row_count
    FROM sequenced
) src
UNPIVOT (
    value FOR metric_name IN (ms_played_device_is_not_null, distinct_listener_ids, row_count)
) unp
