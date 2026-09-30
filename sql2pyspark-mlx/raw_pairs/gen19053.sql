SELECT
    id,
    status,
    delivered_date,
    shipped_date,
    FIRST_VALUE(shipped_date) OVER (PARTITION BY status ORDER BY delivered_date DESC, id) first_shipped_date
FROM shipments
ORDER BY delivered_date;
