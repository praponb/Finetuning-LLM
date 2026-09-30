WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', opened_date) AS opened_date_period,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM accounts
    WHERE account_type IN ('LOAN', 'SAVINGS', 'CHECKING')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, status
    FROM valid
    INTERSECT
    SELECT customer_id, status
    FROM accounts
    WHERE customer_id IN (1, 27)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date_period
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        account_type,
        balance,
        opened_date,
        opened_date_period,
        status_prefix,
        MIN(balance) OVER w_hist AS opened_date_period_month_min_balance,
        AVG(balance) OVER w_part AS balance_avg_90d,
        COUNT(balance) OVER w_hist_2 AS balance_count_30d
    FROM eligible
    WINDOW w_hist AS (PARTITION BY MONTH(opened_date_period), customer_id), w_part AS (ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY status ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
valid_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        account_type,
        balance,
        status_prefix,
        opened_date_period_month_min_balance,
        CASE WHEN opened_date_period_month_min_balance >= 67265 THEN 'High' WHEN opened_date_period_month_min_balance >= 10051 THEN 'Medium' ELSE 'Low' END AS opened_date_period_month_min_balance_band,
        UPPER(account_type) AS account_type_upper
    FROM scored
    WHERE opened_date > DATE '2023-03-06'
),
valid_accounts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(opened_date_period_month_min_balance * 1.07, 2) AS adjusted_opened_date_period_month_min_balance,
        TRIM(account_type) AS clean_account_type
    FROM valid_accounts
    WHERE account_type IN ('SAVINGS', 'LOAN', 'CHECKING')
)
SELECT
    account_type,
    balance,
    status_prefix,
    opened_date_period_month_min_balance,
    opened_date_period_month_min_balance_band,
    account_type_upper,
    CASE WHEN account_type RLIKE '^(LOAN|SAVINGS)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS account_type_zone
FROM valid_accounts_2
ORDER BY account_type_zone
