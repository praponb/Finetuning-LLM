SELECT
    cost,
    delivered_date,
    CONCAT(carrier, ' - ', status) AS carrier_status,
    LAST_DAY(delivered_date) AS delivered_date_month_end
FROM shipments;
