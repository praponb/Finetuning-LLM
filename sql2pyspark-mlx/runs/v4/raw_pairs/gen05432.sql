WITH scored AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY balance NULLS FIRST) AS balance_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS seq_no
    FROM accounts
)
SELECT
    status,
    CASE WHEN balance_cume_dist >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_cume_dist_tier,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(DISTINCT status) AS distinct_status,
    MAX(balance_cume_dist) AS max_balance_cume_dist
FROM scored
GROUP BY status, CASE WHEN balance_cume_dist >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_status DESC NULLS FIRST;
