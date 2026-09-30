SELECT *
FROM (
    SELECT
        id,
        status,
        shipping_fee,
        amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) rnk
    FROM orders
    WHERE status NOT IN ('RETURNED', 'PENDING', 'COMPLETED')
) ranked
WHERE rnk = 1
ORDER BY status ASC;
