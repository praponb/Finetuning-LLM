SELECT id, started_at, country, TO_DATE(started_at) AS started_at_date FROM sessions WHERE browser IS NOT NULL;
