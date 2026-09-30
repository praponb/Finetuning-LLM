WITH exploded AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        entry.product_id,
        entry.qty,
        entry.price,
        entry.promo
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) x AS entry
)
SELECT
    s.id,
    s.customer_id,
    s.product_id,
    s.qty,
    (SELECT MAX(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS max_account_balance
FROM exploded s
ORDER BY id DESC, max_account_balance;
