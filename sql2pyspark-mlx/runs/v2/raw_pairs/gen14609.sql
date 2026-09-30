SELECT SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM shipments WHERE delivered_date < '2025-05-01' OR YEAR(shipped_date) >= 2025;
