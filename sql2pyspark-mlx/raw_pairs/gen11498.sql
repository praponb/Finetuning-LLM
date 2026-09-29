WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(status) AS status_lower
    FROM orders
    WHERE order_date > TO_DATE('2023-04-09')
      AND store_id IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    shipping_fee,
    status_lower,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status, store_id), 4) AS amount_share,
    CORR(shipping_fee, amount) OVER (PARTITION BY channel) AS corr_shipping_fee_amount,
    AVG(discount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_avg_6_prior
FROM filtered
WHERE YEAR(order_date) = 2025;
