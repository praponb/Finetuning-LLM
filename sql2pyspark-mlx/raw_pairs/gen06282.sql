WITH base AS (
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        INITCAP(TRIM(account_type)) AS account_type_title,
        LENGTH(status) AS status_length
    FROM accounts
    WHERE balance > 28000
)
SELECT
    id,
    customer_id,
    account_type,
    opened_date,
    account_type_title,
    NTILE(10) OVER (PARTITION BY customer_id ORDER BY status_length DESC, id) AS status_length_bucket,
    PERCENT_RANK() OVER (PARTITION BY customer_id, account_type ORDER BY status_length NULLS FIRST) AS status_length_percent_rank
FROM base
WHERE opened_date > '2024-07-01';
