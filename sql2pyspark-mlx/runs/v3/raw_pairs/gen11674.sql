SELECT
    status,
    QUARTER(shipped_date) shipped_quarter,
    carrier,
    SORT_ARRAY(COLLECT_SET(status)) status_list,
    COUNT(*) total_count
FROM shipments
WHERE YEAR(delivered_date) = 2024 AND carrier IS NULL AND cost BETWEEN 34 AND 58
GROUP BY status, QUARTER(shipped_date), carrier
