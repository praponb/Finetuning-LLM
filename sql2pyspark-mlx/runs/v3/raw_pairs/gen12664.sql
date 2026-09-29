WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE page != '/product'
),
agg AS (
    SELECT page, ROUND((100 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_purchase
    FROM valid_web_events
    GROUP BY page
)
SELECT page, pct_purchase
FROM agg;
