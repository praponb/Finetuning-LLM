WITH cleaned AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        DATE_TRUNC('MONTH', event_time) AS event_time_period,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 6 AND 11
),
enriched AS (
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.page,
        src.event_time,
        src.duration_sec,
        src.event_time_period,
        src.page_filled,
        p.browser
    FROM cleaned src
    INNER JOIN sessions p
        ON src.session_id = p.id
),
filtered AS (
    SELECT
        *,
        QUARTER(event_time) AS event_time_quarter,
        UPPER(browser) AS browser_upper
    FROM enriched
    WHERE browser IS NOT NULL
      AND page = '/search'
),
kept_keys AS (
    SELECT page, id
    FROM filtered
    EXCEPT
    SELECT page, id
    FROM web_events
    WHERE duration_sec BETWEEN 138 AND 266
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.page = k.page AND s.id = k.id
)
SELECT
    DATE_TRUNC('YEAR', event_time_period) AS year_start,
    event_type,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    ROUND(AVG(event_time_quarter), 2) AS avg_event_time_quarter,
    COUNT(*) AS event_count
FROM kept_rows
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', event_time_period), event_type), (DATE_TRUNC('YEAR', event_time_period)), ())
ORDER BY event_type NULLS LAST;
