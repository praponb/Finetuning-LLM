WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM-dd') opened_date_label
    FROM accounts
    WHERE opened_date >= DATE '2023-03-27'
      AND balance >= 43000
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.amount,
        x.transaction_merchant
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.amount, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance ASC NULLS LAST, id ASC) AS row_num
    FROM latest_match
    QUALIFY row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM deduped
    EXCEPT DISTINCT
    SELECT status, id
    FROM accounts
    WHERE balance <= 18000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, d.age
    FROM reconciled a
    INNER JOIN customers d
        ON a.customer_id = d.id
    WHERE d.name NOT LIKE 'Mia%'
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    status,
    opened_date_label,
    amount,
    transaction_merchant,
    age
FROM enriched
