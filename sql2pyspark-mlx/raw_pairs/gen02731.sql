WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        SUM(balance) OVER w_recent AS running_balance,
        SUM(balance) OVER (PARTITION BY YEAR(opened_date)) AS opened_date_year_sum_balance,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM accounts
    WHERE account_type = 'SAVINGS'
    WINDOW w_recent AS (PARTITION BY customer_id, status ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    customer_id,
    account_type,
    opened_date
FROM with_windows
ORDER BY id DESC NULLS FIRST, customer_id DESC
