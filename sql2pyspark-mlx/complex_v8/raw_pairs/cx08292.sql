SELECT id AS ref_id, commissioned_on AS day
FROM sites
UNION ALL
SELECT id, commissioned_on
FROM sites
