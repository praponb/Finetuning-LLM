SELECT *
FROM (
    SELECT YEAR(shipped_date) shipped_year, ROUND((100 * SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_dhl
    FROM shipments
    WHERE status IN ('RETURNED', 'IN_TRANSIT')
    GROUP BY YEAR(shipped_date)
) agg
WHERE pct_dhl >= 24
ORDER BY shipped_year DESC;
