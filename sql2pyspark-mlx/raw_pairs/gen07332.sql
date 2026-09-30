SELECT * FROM sessions WHERE country IN ('US', 'JP', 'SG') AND HOUR(started_at) BETWEEN 7 AND 13 AND country != 'UK' ORDER BY started_at DESC;
