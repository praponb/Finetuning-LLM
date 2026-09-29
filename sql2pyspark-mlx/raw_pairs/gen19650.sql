WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id, status ORDER BY balance DESC) AS latest_rank
        FROM not_flagged
    ) x
    WHERE latest_rank = 1
),
latest_accounts AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance DESC NULLS FIRST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance) OVER w_recent AS running_balance,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        SUM(balance) OVER w_hist AS cumulative_balance
    FROM latest_accounts
    WINDOW w_recent AS (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance_avg_2_prior) OVER (PARTITION BY status ORDER BY running_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_avg_2_prior,
        NTILE(3) OVER (PARTITION BY customer_id, status ORDER BY running_balance DESC, id) AS running_balance_bucket
    FROM scored
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_merchant
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.segment,
        c.name,
        c.signup_date
    FROM with_last a
    LEFT JOIN customers c
        ON a.customer_id = c.id
    WHERE c.age <> 50
)
-- Grouped metrics
SELECT
    status,
    CASE WHEN running_balance >= 424114 THEN 'LARGE' ELSE 'SMALL' END AS running_balance_tier,
    SUM(CASE WHEN transaction_merchant = 'Grab' THEN running_balance_avg_2_prior ELSE 0 END) AS grab_running_balance_avg_2_prior,
    COUNT(*) FILTER (WHERE transaction_merchant = 'Amazon') AS amazon_rows,
    MAX(running_balance) FILTER (WHERE segment = 'SMB') AS smb_max_running_balance,
    PERCENTILE_APPROX(running_balance, ARRAY(0.25, 0.5, 0.75)) AS running_balance_quartiles,
    ROUND(SUM(running_balance_avg_2_prior) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS running_balance_avg_2_prior_per_open,
    RANK() OVER (PARTITION BY status ORDER BY COUNT(*) FILTER (WHERE transaction_merchant = 'Amazon') DESC) AS amazon_rows_rank
FROM enriched
GROUP BY 1, 2
HAVING COUNT(*) FILTER (WHERE transaction_merchant = 'Amazon') > 1
ORDER BY status DESC;
