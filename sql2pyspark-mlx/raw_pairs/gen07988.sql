SELECT
    shipped_date,
    carrier,
    DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date,
    INITCAP(TRIM(status)) AS status_title
FROM shipments
WHERE carrier <> 'Kerry'
ORDER BY shipped_date DESC;
