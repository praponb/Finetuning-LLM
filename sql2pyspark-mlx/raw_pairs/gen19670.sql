SELECT
    carrier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count,
    SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END) AS flash_count
FROM shipments
GROUP BY carrier
HAVING SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END) > 52
ORDER BY flash_count DESC;
