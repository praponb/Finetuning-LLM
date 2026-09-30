SELECT *, DATE_TRUNC('MONTH', started_at) AS started_at_period FROM sessions WHERE browser IN ('Firefox', 'Edge', 'Chrome') AND country <> 'SG' ORDER BY id DESC;
