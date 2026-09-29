WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY page, event_type ORDER BY duration_sec ASC, id ASC) AS duration_sec_bucket,
        COUNT(duration_sec) OVER (PARTITION BY session_id) AS session_id_count_duration_sec
    FROM web_events
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec_bucket) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_avg_30d,
        SUM(session_id_count_duration_sec) OVER (PARTITION BY page ORDER BY session_id_count_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_id_count_duration_sec
    FROM with_windows
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.browser,
        d.country,
        d.device
    FROM enriched src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        HOUR(event_time) AS event_time_hour,
        SUBSTRING(page, 1, 3) AS page_prefix
    FROM enriched_web_events
    WHERE event_time > TO_DATE('2024-02-25')
      AND session_id_count_duration_sec IS NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY event_type, session_id ORDER BY running_session_id_count_duration_sec DESC NULLS LAST) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_session_id_count_duration_sec >= 75 THEN 'LARGE' ELSE 'SMALL' END AS running_session_id_count_duration_sec_tier,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    MAX(event_time) AS ended_at,
    COUNT(*) FILTER (WHERE device = 'tablet') AS tablet_rows
FROM latest
GROUP BY CUBE (CASE WHEN running_session_id_count_duration_sec >= 75 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', event_time))
ORDER BY search_rows DESC NULLS FIRST
