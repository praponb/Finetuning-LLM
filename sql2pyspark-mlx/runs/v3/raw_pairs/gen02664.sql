SELECT event_type, cnt FROM (SELECT event_type, COUNT(*) cnt FROM web_events WHERE event_time >= '2024-11-20' GROUP BY event_type) agg WHERE cnt > 9 ORDER BY cnt DESC
