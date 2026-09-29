WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(status, ' ', account_type) AS status_account_type
    FROM accounts
    WHERE opened_date > DATE '2023-06-03'
      AND customer_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) = 1
),
latest_accounts AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY account_type ORDER BY balance NULLS FIRST) = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, status
    FROM latest_accounts
    INTERSECT ALL
    SELECT customer_id, status
    FROM accounts
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_accounts s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            account_type,
            balance
        FROM kept_rows
    ) src
    PIVOT (
        AVG(balance) AS amount
        FOR account_type IN ('CHECKING' AS checking, 'LOAN' AS loan)
    )
)
SELECT
    status,
    checking,
    loan
FROM wide
ORDER BY loan DESC NULLS FIRST
