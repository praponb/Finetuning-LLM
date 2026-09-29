SELECT zone AS label, commissioned_on AS day
FROM sites
UNION ALL
SELECT zone AS label, commissioned_on AS day
FROM sites
WHERE installed_kw >= 96.9
ORDER BY day
