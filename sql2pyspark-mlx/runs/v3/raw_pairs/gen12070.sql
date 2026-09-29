SELECT
    id,
    amount,
    store_id,
    ROUND(amount, 2) amount_rounded
FROM orders
WHERE customer_id IS NOT NULL;
