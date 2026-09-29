WITH filtered AS (
    SELECT *, COALESCE(balance, 0) AS balance_filled
    FROM accounts
    WHERE opened_date > TO_DATE('2023-05-31')
      AND customer_id IS NOT NULL
)
SELECT
    src.id,
    src.customer_id,
    src.balance,
    src.status,
    b.segment
FROM filtered src
JOIN customers b
    ON src.customer_id = b.id;
