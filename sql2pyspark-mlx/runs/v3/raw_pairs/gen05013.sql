WITH ValidOrders AS (
    SELECT *
    FROM orders
    WHERE NOT (discount >= 0.25 AND shipping_fee < 22 AND channel <> 'store')
),
Agg AS (
    SELECT channel, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM ValidOrders
    GROUP BY channel
)
SELECT channel, stddev_amount
FROM Agg
WHERE stddev_amount > 60
ORDER BY 2;
