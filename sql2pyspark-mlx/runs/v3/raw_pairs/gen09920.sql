SELECT
    id,
    carrier,
    INITCAP(TRIM(status)) AS status_title,
    DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
FROM shipments
WHERE YEAR(shipped_date) >= 2025
ORDER BY 4, id DESC
