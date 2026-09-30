SELECT
    DATE_TRUNC('MONTH', shipped_date) AS shipped_month,
    QUARTER(delivered_date) AS delivered_quarter,
    DATE_TRUNC('MONTH', delivered_date) AS delivered_month,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS total_count
FROM shipments
GROUP BY DATE_TRUNC('MONTH', shipped_date), QUARTER(delivered_date), DATE_TRUNC('MONTH', delivered_date);
