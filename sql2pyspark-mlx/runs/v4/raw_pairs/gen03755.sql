WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN balance >= 84000 THEN 'High' WHEN balance >= 18000 THEN 'Medium' ELSE 'Low' END balance_band,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM accounts
    WHERE opened_date > DATE '2024-02-01'
      AND customer_id IN (10, 15, 22)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_band,
        days_since_opened_date,
        SUM(days_since_opened_date) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_days_since_opened_date,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY days_since_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance,
        NTILE(3) OVER (PARTITION BY customer_id, status ORDER BY days_since_opened_date, id) days_since_opened_date_bucket,
        LAG(days_since_opened_date, 1) OVER (PARTITION BY account_type ORDER BY balance, id) prev_days_since_opened_date,
        DENSE_RANK() OVER (ORDER BY days_since_opened_date DESC NULLS FIRST) days_since_opened_date_dense_rank
    FROM valid
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_band,
        days_since_opened_date,
        running_days_since_opened_date,
        days_since_opened_date_dense_rank,
        CONCAT_WS(',', account_type, status) account_type_status_key,
        COALESCE(status, 'NONE') status_clean
    FROM scored
    WHERE opened_date > TO_DATE('2023-05-10')
      AND days_since_opened_date IS NOT NULL
)
SELECT
    id,
    balance,
    opened_date,
    status,
    days_since_opened_date,
    days_since_opened_date_dense_rank,
    account_type_status_key,
    CASE WHEN status RLIKE '^(FROZEN|CLOSED)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS status_zone
FROM cleaned
ORDER BY status NULLS FIRST, days_since_opened_date_dense_rank
