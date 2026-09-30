SELECT CASE WHEN sessions.country IN ('JP', 'UK') THEN 'tier_1' ELSE 'other' END AS country_group, sessions.customer_id, MAX(web_events.duration_sec) - MIN(web_events.duration_sec) AS duration_sec_range
FROM web_events
FULL OUTER JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.started_at >= '2023-04-14'
GROUP BY CASE WHEN sessions.country IN ('JP', 'UK') THEN 'tier_1' ELSE 'other' END, sessions.customer_id
ORDER BY duration_sec_range DESC
