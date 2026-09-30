WITH valid AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit
    FROM accounts
    WHERE opened_date > TO_DATE('2023-06-04')
      AND status IS NOT NULL
),
metrics AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        is_credit,
        LAG(balance) OVER w_ordered AS prev_balance,
        LEAD(balance) OVER w_ordered AS next_balance,
        LAG(is_credit) OVER w_ordered AS prev_is_credit,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM valid
    WINDOW w_ordered AS (PARTITION BY status ORDER BY balance, id)
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.is_credit,
        s.prev_balance,
        s.next_balance,
        s.prev_is_credit,
        s.seq_no,
        p.city
    FROM not_flagged s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
eligible AS (
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type IN ('CREDIT', 'REFUND', 'DEBIT')
        )
)
SELECT
    id,
    prev_balance,
    seq_no,
    NTILE(3) OVER w_part AS prev_is_credit_bucket,
    ROW_NUMBER() OVER w_ordered AS row_seq
FROM eligible
WHERE customer_id IS NULL
WINDOW w_part AS (ORDER BY prev_is_credit, id), w_ordered AS (PARTITION BY customer_id ORDER BY prev_balance, id)
