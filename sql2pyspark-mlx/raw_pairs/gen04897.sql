SELECT *, CASE WHEN device = 'tablet' THEN 1 ELSE 0 END is_tablet
FROM sessions
WHERE country IN ('SG', 'UK') OR TO_DATE(started_at) >= '2024-09-01'
ORDER BY device;
