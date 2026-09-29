SELECT
    id,
    delivered_date,
    status,
    LAST_DAY(delivered_date) AS delivered_date_month_end
FROM shipments
ORDER BY delivered_date_month_end, id;
