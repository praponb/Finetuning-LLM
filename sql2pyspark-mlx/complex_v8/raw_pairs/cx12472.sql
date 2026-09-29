SELECT id AS ref_id, commissioned_on AS event_date
FROM sites
WHERE installed_kw < 386.6
UNION ALL
SELECT id AS ref_id, commissioned_on AS event_date
FROM sites
