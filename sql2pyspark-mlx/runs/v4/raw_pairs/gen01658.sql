SELECT
    id,
    delivered_date,
    status,
    carrier,
    LAST_DAY(shipped_date) AS shipped_date_month_end,
    DATE_ADD(shipped_date, 84) AS shipped_date_plus_84d
FROM shipments
ORDER BY 5 DESC;
