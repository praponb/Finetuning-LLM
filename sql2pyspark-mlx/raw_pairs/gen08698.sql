WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance DESC NULLS LAST, id ASC) = 1
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.opened_date,
    s.status,
    (SELECT SUM(t2.shipping_fee) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS sum_order_shipping_fee
FROM latest s;
