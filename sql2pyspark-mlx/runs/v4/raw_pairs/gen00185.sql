SELECT *
FROM (
    SELECT
        id,
        customer_id,
        discount,
        amount,
        shipping_fee
    FROM orders
) src
UNPIVOT (
    val FOR kpi IN (discount AS DISCOUNT_VAL, amount AS AMOUNT_VAL, shipping_fee AS SHIPPING_FEE_VAL)
)
