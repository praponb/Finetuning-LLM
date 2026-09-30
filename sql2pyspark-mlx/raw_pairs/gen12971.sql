WITH valid AS (
    SELECT *, CAST(txn_time AS DATE) AS txn_time_day
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Shell'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY amount NULLS FIRST) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
joined AS (
    SELECT
        e.*,
        b.balance,
        b.account_type,
        b.opened_date
    FROM ranked e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
ranked_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, account_type ORDER BY balance NULLS FIRST, id) AS dedup_rank
    FROM joined
    QUALIFY dedup_rank = 1
)
SELECT
    CASE WHEN balance >= 87000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    merchant,
    PERCENTILE_APPROX(balance, 0.9) AS balance_p90,
    MAX(txn_time) AS last_txn_time,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM ranked_transactions
GROUP BY CASE WHEN balance >= 87000 THEN 'PREMIUM' ELSE 'STANDARD' END, merchant
ORDER BY merchant NULLS LAST
