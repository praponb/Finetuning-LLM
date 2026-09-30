SELECT *, DATEDIFF(CURRENT_DATE(), started_at) / 30 AS months_since_started_at FROM sessions;
