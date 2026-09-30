SELECT
    id,
    carrier,
    delivered_date,
    ADD_MONTHS(shipped_date, 1) AS shipped_date_plus_months,
    DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date
FROM shipments
WHERE status IN ('IN_TRANSIT', 'RETURNED', 'DELIVERED') AND status NOT IN ('RETURNED', 'IN_TRANSIT', 'LOST')
