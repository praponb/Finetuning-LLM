SELECT *, DATE_TRUNC('year', started_at) AS started_at_period, CASE WHEN browser = 'Edge' THEN 1 ELSE 0 END AS is_edge FROM sessions WHERE browser <> 'Chrome'
