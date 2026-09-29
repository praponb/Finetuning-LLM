SELECT id AS ref_id, zone
FROM sites
UNION ALL
SELECT id, site_name
FROM sites
ORDER BY ref_id
