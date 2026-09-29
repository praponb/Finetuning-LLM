WITH latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY order_date) AS rank_in_group
        FROM orders
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    order_date,
    discount,
    shipping_fee,
    rank_in_group
FROM latest
