SELECT *
FROM (
    SELECT
        store_id,
        status,
        shipping_fee
    FROM orders
) src
PIVOT (
    AVG(shipping_fee) AS value_sum,
    COUNT(1) AS n
    FOR status IN ('RETURNED', 'CANCELLED')
);
