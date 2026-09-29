SELECT country FROM sessions WHERE browser <> 'Edge' UNION ALL SELECT country FROM sessions WHERE HOUR(started_at) BETWEEN 6 AND 13;
