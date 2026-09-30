WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts c
    LATERAL VIEW OUTER EXPLODE(c.items) x AS elem
)
SELECT
    s.id,
    s.customer_id,
    s.product_id,
    s.qty,
    s.price,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM flattened s
ORDER BY avg_account_balance;
