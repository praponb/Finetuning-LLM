WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE (amount <= 1520 AND shipping_fee BETWEEN 26 AND 35) OR amount >= 790
),
agg AS (
    SELECT store_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list, MIN(amount) AS min_amount
    FROM filtered_orders
    GROUP BY store_id
)
SELECT *
FROM agg
WHERE min_amount > 810;
