SELECT
    MONTH(s.started_at) AS started_month,
    CASE WHEN s.browser IN ('Edge', 'Safari') THEN 'core' ELSE 'other' END AS browser_group,
    ROUND(AVG(we.duration_sec), 2) AS avg_duration_sec,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.page))) AS page_values
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.page IN ('/checkout', '/cart')
GROUP BY MONTH(s.started_at), CASE WHEN s.browser IN ('Edge', 'Safari') THEN 'core' ELSE 'other' END;
