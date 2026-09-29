SELECT device, started_at, DATE_TRUNC('MONTH', started_at) AS started_at_period FROM sessions ORDER BY started_at_period;
