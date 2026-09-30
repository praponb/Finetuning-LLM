WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM sessions
    ) t
    WHERE dedup_rank = 1
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    browser,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT(*) AS event_count,
    MIN(started_at) AS first_started_at,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SIZE(COLLECT_SET(device)) AS num_devices,
    MAX(started_at) AS last_started_at
FROM latest
GROUP BY DATE_TRUNC('YEAR', started_at), browser
ORDER BY year_start ASC NULLS LAST;
