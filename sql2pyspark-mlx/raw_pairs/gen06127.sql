SELECT
    id,
    status,
    carrier,
    cost,
    DATEDIFF(CURRENT_DATE(), delivered_date) / 365 AS years_since_delivered_date,
    SUBSTRING(carrier, 1, 3) AS carrier_prefix
FROM shipments;
