WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN balance >= 64000 THEN 'High' WHEN balance >= 31000 THEN 'Medium' ELSE 'Low' END AS balance_band,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-02-27'
      AND status IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        resolved_status,
        LEAD(balance) OVER w_grp AS next_balance,
        ROW_NUMBER() OVER w_grp AS row_seq,
        NTILE(5) OVER (PARTITION BY MONTH(opened_date), customer_id ORDER BY balance DESC, id) AS balance_bucket
    FROM valid
    WINDOW w_grp AS (PARTITION BY status ORDER BY balance, id)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (9, 11, 14, 26)
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM eligible
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY next_balance) = 1
)
SELECT
    id,
    customer_id,
    account_type,
    next_balance,
    balance_bucket
FROM latest;
