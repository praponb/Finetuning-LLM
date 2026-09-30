WITH base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) / 365.25 AS years_since_opened_date,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE opened_date >= DATE '2023-03-09'
      AND balance > 32000
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
enriched AS (
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.status,
        s.years_since_opened_date,
        c.country
    FROM kept s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
not_flagged AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < '2023-06-04'
        )
)
SELECT
    CASE WHEN years_since_opened_date >= 76 THEN 'LARGE' ELSE 'SMALL' END AS years_since_opened_date_tier,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END) AS checking_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count
FROM not_flagged
GROUP BY CASE WHEN years_since_opened_date >= 76 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_balance DESC;
