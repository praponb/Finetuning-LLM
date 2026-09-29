WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        INITCAP(TRIM(account_type)) AS account_type_title,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-07-17')
      AND MONTH(opened_date) IN (3, 5, 6)
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.merchant
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.signup_date,
        p.credit_limit,
        p.name
    FROM with_last e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        resolved_status,
        merchant,
        signup_date,
        credit_limit,
        name,
        CASE WHEN merchant IN ('Grab', 'Amazon') THEN 'Priority' ELSE 'Other' END AS merchant_group,
        COALESCE(status, 'UNKNOWN') AS resolved_status_2
    FROM enriched
    WHERE merchant = 'Amazon'
      AND status IS DISTINCT FROM 'OPEN'
),
enriched_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC) AS credit_limit_dense_rank,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY credit_limit, id) AS credit_limit_bucket,
        ROW_NUMBER() OVER (ORDER BY balance, id) AS seq_no
    FROM filtered
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.merchant,
    s.signup_date,
    s.credit_limit,
    s.name,
    CASE WHEN name RLIKE '^(Emma Brown|bob smith)$' THEN 'GROUP_B' WHEN name RLIKE '^(Alice Wong|  Carol Diaz)$' THEN 'EXTENDED' ELSE 'CORE' END AS name_zone,
    (SELECT MIN(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS min_account_balance
FROM enriched_accounts s
ORDER BY id DESC
