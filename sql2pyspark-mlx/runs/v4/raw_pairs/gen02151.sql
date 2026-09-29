SELECT
    id,
    carrier,
    cost,
    LOWER(status) AS status_lower
FROM shipments;
