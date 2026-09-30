SELECT
    id,
    store_id,
    metric,
    val
FROM (
    SELECT
        id,
        store_id,
        amount,
        shipping_fee,
        discount
    FROM orders
) src
UNPIVOT (
    val FOR metric IN (amount AS AMOUNT, shipping_fee AS SHIPPING_FEE, discount AS DISCOUNT)
) long_rows;
