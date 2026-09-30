WITH valid AS (
    SELECT *, DAYOFWEEK(order_date) AS order_date_dayofweek
    FROM orders
    WHERE channel IS DISTINCT FROM 'web'
      AND order_date >= TO_DATE('2023-05-06')
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    discount,
    order_date_dayofweek,
    NTILE(4) OVER (PARTITION BY status ORDER BY discount, id) AS discount_bucket,
    LEAD(discount, 2) OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS next_discount
FROM valid;
