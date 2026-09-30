WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        LENGTH(page) AS page_length
    FROM web_events
    WHERE page IS NULL
)
SELECT id, page
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY page_length DESC NULLS LAST, id) = 1;
