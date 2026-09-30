WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        SUBSTRING(account_type, 1, 4) AS account_type_prefix,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE status IN ('OPEN', 'FROZEN')
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    account_type,
    opened_date,
    account_type_prefix,
    resolved_status,
    ROW_NUMBER() OVER (ORDER BY opened_date, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY opened_date DESC, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS seq_no_2
FROM filtered;
