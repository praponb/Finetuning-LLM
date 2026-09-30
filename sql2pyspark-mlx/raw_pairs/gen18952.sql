WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        TRIM(account_type) AS clean_account_type,
        COALESCE(status, 'NONE') AS status_filled
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'CLOSED'
      AND balance <= 58000
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IN ('Lazada', 'Shell', 'Amazon')
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY balance, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_date) ORDER BY balance, id) AS seq_no_2
    FROM kept
    WHERE opened_date < '2024-04-01'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.is_flagged
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY seq_no DESC NULLS FIRST, id) AS rank_in_group
    FROM eligible
    QUALIFY rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance, 2) OVER (ORDER BY seq_no_2, id) AS prev_balance,
        SUM(seq_no) OVER (PARTITION BY MONTH(opened_date) ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY seq_no DESC, id) AS seq_no_bucket,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('month', opened_date), status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no
    FROM latest
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rank_in_group_2
        FROM with_windows
    ) t
    WHERE rank_in_group_2 = 1
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.clean_account_type,
    s.status_filled,
    s.seq_no_2,
    (SELECT MAX(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM deduped s;
