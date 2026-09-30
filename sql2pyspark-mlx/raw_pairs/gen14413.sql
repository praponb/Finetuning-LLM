SELECT id, country, DATE_TRUNC('year', started_at) AS started_at_period, DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label FROM sessions;
