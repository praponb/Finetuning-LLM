WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date <= DATE_ADD(sub.end_date, 10)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY monthly_fee DESC NULLS FIRST) AS latest_rank
    FROM attributed
    QUALIFY latest_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= '2025-08-25'
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(monthly_fee, 1) OVER (PARTITION BY account_type ORDER BY latest_rank ASC, id ASC, subscription_id ASC) AS prev_monthly_fee,
        LEAD(monthly_fee, 1) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC, subscription_id ASC) AS next_monthly_fee
    FROM eligible
),
attributed_accounts AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_windows src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date < sub.end_date + INTERVAL 14 DAYS
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY subscription_plan ORDER BY monthly_fee DESC, id ASC, subscription_id ASC, subscription_id_2 ASC) AS rank_in_group
        FROM attributed_accounts
    ) t
    WHERE rank_in_group = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            account_type,
            subscription_id
        FROM latest
    ) src
    PIVOT (
        SUM(subscription_id) AS total
        FOR account_type IN ('LOAN', 'SAVINGS', 'CHECKING')
    )
)
SELECT
    status,
    LOAN,
    SAVINGS,
    CHECKING,
    CASE WHEN status RLIKE '^(OPEN|FROZEN)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS status_zone
FROM wide
