SELECT event_type, ROUND((100 * SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_click FROM web_events GROUP BY event_type ORDER BY pct_click ASC;
