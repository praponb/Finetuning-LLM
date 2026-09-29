WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        CEIL(balance) AS balance_whole
    FROM accounts
    WHERE opened_date > '2024-08-01'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type IN ('FEE', 'CREDIT')
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance_whole) OVER w_grp AS cumulative_balance_whole,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM eligible
    WINDOW w_grp AS (PARTITION BY account_type, customer_id ORDER BY balance_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY customer_id ORDER BY balance DESC, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY balance DESC) AS balance_cume_dist,
        SUM(seq_no) OVER (PARTITION BY customer_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id, account_type ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM metrics
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN balance_cume_dist >= 0.82 THEN 'LARGE' ELSE 'SMALL' END AS balance_cume_dist_tier,
        seq_no,
        SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
        MAX(balance_whole) AS max_balance_whole,
        ROUND(SUM(balance_cume_dist) / NULLIF(COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END), 0), 2) AS balance_cume_dist_per_savings,
        ROUND(AVG(balance_whole), 2) AS avg_balance_whole
    FROM scored
    GROUP BY CASE WHEN balance_cume_dist >= 0.82 THEN 'LARGE' ELSE 'SMALL' END, seq_no
    HAVING avg_balance_whole < 0
)
SELECT
    balance_cume_dist_tier,
    seq_no,
    account_type_list,
    balance_cume_dist_per_savings,
    avg_balance_whole
FROM summary
ORDER BY seq_no DESC NULLS LAST;
