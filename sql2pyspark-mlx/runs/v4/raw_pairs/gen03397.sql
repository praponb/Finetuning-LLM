WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(duration_sec * 0.9, 2) AS duration_sec_scaled,
        CONCAT(event_type, ', ', page) AS event_type_page,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        page,
        event_type,
        duration_sec
    FROM valid
) src
PIVOT (
    AVG(duration_sec) AS value_sum,
    COUNT(1) AS n
    FOR event_type IN ('search', 'page_view')
);
