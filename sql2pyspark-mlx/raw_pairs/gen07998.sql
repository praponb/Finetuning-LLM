SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM shipments
) src
PIVOT (
    AVG(cost) AS value_sum
    FOR status IN ('IN_TRANSIT' AS in_transit, 'DELIVERED' AS delivered)
);
