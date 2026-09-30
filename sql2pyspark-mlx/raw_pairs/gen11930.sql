WITH src AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM orders
    WHERE discount BETWEEN 0.15 AND 0.24
),
agg AS (
    SELECT status, COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
    FROM src
    GROUP BY status
)
SELECT *
FROM agg
ORDER BY 2 DESC
