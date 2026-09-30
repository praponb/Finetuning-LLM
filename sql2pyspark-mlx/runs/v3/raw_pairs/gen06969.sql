WITH valid_web_events AS (
    SELECT *, LOWER(page) AS page_lower
    FROM web_events
    WHERE duration_sec = 453
),
agg AS (
    SELECT session_id, SORT_ARRAY(COLLECT_SET(page)) AS page_list
    FROM valid_web_events
    GROUP BY session_id
)
SELECT *
FROM agg
ORDER BY 1 DESC
