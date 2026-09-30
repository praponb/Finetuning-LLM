SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM shipments
) src
PIVOT (
    SUM(cost) AS total,
    COUNT(1) AS n
    FOR status IN ('DELIVERED', 'RETURNED', 'IN_TRANSIT')
);
