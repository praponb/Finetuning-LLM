WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'Priority' ELSE 'Other' END AS status_group
    FROM accounts
    WHERE status IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date <= DATE_ADD(sub.end_date, 3)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (2, 28, 30)
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status, id
    FROM not_flagged
    INTERSECT ALL
    SELECT status, id
    FROM accounts
    WHERE account_type != 'LOAN'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan, status), 4) AS monthly_fee_share,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY YEAR(opened_date), status), 4) AS monthly_fee_share_2
    FROM eligible
)
SELECT
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.status_group,
    s.subscription_id,
    s.plan,
    s.monthly_fee_share_2,
    CASE WHEN status RLIKE '^(OPEN|FROZEN)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM enriched s
ORDER BY max_order_amount DESC NULLS LAST
