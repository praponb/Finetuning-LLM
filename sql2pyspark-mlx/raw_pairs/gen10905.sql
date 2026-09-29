WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount,
        shipping_fee,
        GREATEST(discount, amount) AS greatest_discount_amount
    FROM orders
    WHERE order_date >= DATE '2023-05-14'
      AND store_id IN (4, 6, 22, 26)
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    store_id,
    order_date,
    amount
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC NULLS FIRST, id) = 1
