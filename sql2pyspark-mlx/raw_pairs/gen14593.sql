WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        p.country,
        p.city,
        p.signup_date
    FROM accounts s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.country,
        s.city,
        x.amount,
        x.txn_type
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.signup_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_merchant,
        latest.transaction_txn_type
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        country,
        txn_type,
        MIN(balance) OVER w_part AS last4_min_balance,
        MAX(balance) OVER w_part AS last4_max_balance,
        AVG(balance) OVER w_part AS last4_avg_balance,
        NTILE(10) OVER (PARTITION BY city ORDER BY amount DESC, id) AS amount_bucket
    FROM with_last
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        country,
        txn_type,
        last4_min_balance,
        last4_max_balance,
        last4_avg_balance,
        CASE WHEN country IN ('DE', 'TH') THEN 'Priority' ELSE 'Other' END AS country_group
    FROM with_windows
    WHERE country IS NOT NULL
      AND txn_type IS DISTINCT FROM 'CREDIT'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT ALL
    SELECT id
    FROM accounts
    WHERE balance < 66000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    customer_id,
    country,
    txn_type,
    last4_min_balance,
    last4_avg_balance,
    country_group,
    CASE WHEN txn_type RLIKE '^(REFUND|DEBIT)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS txn_type_zone
FROM kept_rows
ORDER BY id, last4_avg_balance;
