WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date BETWEEN sub.start_date AND sub.end_date
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.subscription_id,
        s.plan,
        top1.amount
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY amount DESC, id, subscription_id) AS dedup_rank
    FROM latest_match
    QUALIFY dedup_rank = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.subscription_id,
        s.plan,
        top1.transaction_merchant,
        top1.transaction_amount
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN subscription_id >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END AS subscription_id_tier,
    SUM(CASE WHEN transaction_merchant = '7-Eleven' THEN subscription_id ELSE 0 END) AS 7_eleven_subscription_id,
    COUNT(*) AS event_count
FROM with_last
GROUP BY CASE WHEN subscription_id >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING 7_eleven_subscription_id > 0
ORDER BY 7_eleven_subscription_id NULLS LAST;
