SELECT
    id,
    delivered_date,
    DATEDIFF(CURRENT_DATE(), shipped_date) / 365 AS years_since_shipped_date,
    LOWER(status) AS status_lower
FROM shipments;
