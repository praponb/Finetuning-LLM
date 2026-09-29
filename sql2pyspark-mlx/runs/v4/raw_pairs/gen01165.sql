SELECT *
FROM sessions
WHERE browser <> 'Chrome' AND device IN ('tablet', 'mobile')
ORDER BY device;
