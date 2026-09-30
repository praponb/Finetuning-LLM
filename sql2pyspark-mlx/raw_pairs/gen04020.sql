SELECT
    id,
    status,
    measure,
    val
FROM (
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        amount
    FROM orders
) src
UNPIVOT (
    val FOR measure IN (discount, shipping_fee, amount)
);
