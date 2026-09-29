WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(txn_time) txn_time_date,
        HOUR(txn_time) txn_time_hour
    FROM transactions
    WHERE txn_time >= DATE '2023-04-29'
),
joined AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        b.status,
        b.account_type,
        b.opened_date
    FROM base a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.merchant,
        s.is_flagged,
        s.txn_time_hour,
        s.status,
        s.opened_date,
        p.balance,
        p.opened_date AS account_opened_date
    FROM joined s
    LEFT JOIN accounts p
        ON s.account_id = p.id
    WHERE p.account_type = 'SAVINGS'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY MONTH(account_opened_date), account_id ORDER BY account_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        LAST_VALUE(merchant, TRUE) OVER (ORDER BY account_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_merchant,
        SUM(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance_2,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY MONTH(account_opened_date), merchant ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_merchant
    FROM with_dims
),
enriched AS (
    -- Attach accounts attributes
    SELECT a.*, p2.status AS account_status
    FROM scored a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    PERCENTILE_APPROX(balance, 0.5) AS balance_p50,
    APPROX_PERCENTILE(running_balance, 0.95) AS running_balance_p95,
    ROUND(SUM(running_balance_2) / NULLIF(COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END), 0), 2) AS running_balance_2_per_amazon,
    SUM(CASE WHEN merchant = 'Amazon' THEN running_balance ELSE 0 END) AS amazon_running_balance
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', opened_date)
ORDER BY amazon_running_balance NULLS FIRST;
