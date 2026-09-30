SELECT CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END started_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.event_type))) event_type_values
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
WHERE s.device = 'desktop'
GROUP BY CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END
