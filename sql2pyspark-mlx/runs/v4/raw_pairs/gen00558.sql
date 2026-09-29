WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY days_since_opened_date NULLS FIRST) days_since_opened_date_cume_dist,
        SUM(days_since_opened_date) OVER w_ordered running_days_since_opened_date,
        SUM(days_since_opened_date) OVER (PARTITION BY customer_id ORDER BY days_since_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_days_since_opened_date_2,
        SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance
    FROM valid
    WINDOW w_ordered AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance DESC NULLS FIRST, id) AS rn
        FROM metrics
    ) x
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        days_since_opened_date_cume_dist,
        balance
    FROM latest
) src
UNPIVOT (
    val FOR metric IN (days_since_opened_date_cume_dist AS DAYS_SINCE_OPENED_DATE_CUME_DIST, balance AS BALANCE_VAL)
);
