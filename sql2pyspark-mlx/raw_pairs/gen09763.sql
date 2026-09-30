WITH not_flagged AS (
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
)
SELECT
    CASE WHEN balance >= 74000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS balance_per_checking,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2
FROM not_flagged
GROUP BY 1
ORDER BY cnt_2 DESC;
