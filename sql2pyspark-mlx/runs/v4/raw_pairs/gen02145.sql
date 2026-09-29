WITH src AS (
    SELECT *, ROUND(discount, 2) AS discount_rounded
    FROM orders
    WHERE amount BETWEEN 940 AND 1400 AND discount BETWEEN 0.15 AND 0.23
),
agg AS (
    SELECT QUARTER(order_date) AS order_quarter, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, ROUND(AVG(amount), 2) AS avg_amount
    FROM src
    GROUP BY QUARTER(order_date)
)
SELECT order_quarter, channel_list, avg_amount
FROM agg
WHERE avg_amount > 32
