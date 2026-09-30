SELECT
    id,
    customer_id,
    opened_date,
    status
FROM accounts
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date NULLS LAST, id) = 1;
