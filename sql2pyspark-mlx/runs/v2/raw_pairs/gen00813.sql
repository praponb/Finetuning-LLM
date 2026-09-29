SELECT *
FROM (
    SELECT channel, MAX(shipping_fee) AS max_shipping_fee, AVG(discount) AS avg_discount
    FROM orders
    WHERE channel IN ('web', 'store')
    GROUP BY channel
) AS x
WHERE avg_discount < 0.14
