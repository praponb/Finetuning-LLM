WITH kept AS (
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
),
with_latest AS (
    SELECT
        s.id,
        s.name,
        s.country,
        s.city,
        s.signup_date,
        x.status
    FROM kept s
    JOIN LATERAL (
            SELECT c.status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    SELECT
        *,
        UPPER(country) country_upper,
        SUBSTRING(status, 1, 2) status_prefix,
        COALESCE(city, 'N/A') resolved_city
    FROM with_latest
    WHERE city IS NOT NULL
      AND status IN ('OPEN', 'CLOSED', 'FROZEN')
),
with_latest_customers AS (
    SELECT
        s.id,
        s.name,
        s.country,
        s.resolved_city,
        x.account_balance,
        x.account_account_type
    FROM base s
    JOIN LATERAL (
            SELECT c.balance AS account_balance, c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    SELECT
        id,
        account_account_type,
        NTILE(10) OVER w_part account_balance_bucket,
        MAX(account_balance) OVER (PARTITION BY account_account_type) account_account_type_max_account_balance,
        COUNT(account_balance) OVER (PARTITION BY country) country_count_account_balance,
        ROW_NUMBER() OVER (PARTITION BY country, account_account_type ORDER BY account_balance DESC, id) row_seq,
        LAG(account_balance, 1) OVER (PARTITION BY account_account_type ORDER BY account_balance, id) prev_account_balance
    FROM with_latest_customers
    WINDOW w_part AS (ORDER BY account_balance, id)
),
with_windows_customers AS (
    SELECT
        *,
        LEAD(account_account_type_max_account_balance) OVER (PARTITION BY account_account_type ORDER BY account_account_type_max_account_balance, id) next_account_account_type_max_account_balance,
        LAG(account_balance_bucket) OVER (PARTITION BY account_account_type ORDER BY account_account_type_max_account_balance, id) prev_account_balance_bucket,
        LEAD(account_balance_bucket) OVER (PARTITION BY account_account_type ORDER BY account_account_type_max_account_balance, id) next_account_balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_account_type ORDER BY account_account_type_max_account_balance, id) event_seq
    FROM with_windows
),
enriched AS (
    SELECT
        *,
        MIN(prev_account_balance) OVER w_ordered running_min_prev_account_balance,
        MAX(prev_account_balance) OVER w_ordered running_max_prev_account_balance,
        AVG(prev_account_balance) OVER w_ordered running_avg_prev_account_balance,
        SUM(prev_account_balance) OVER w_ordered running_sum_prev_account_balance,
        SUM(country_count_account_balance) OVER (PARTITION BY account_account_type ORDER BY account_account_type_max_account_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_country_count_account_balance
    FROM with_windows_customers
    WHERE country_count_account_balance <= 13
    WINDOW w_ordered AS (PARTITION BY account_account_type ORDER BY prev_account_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    account_account_type,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles
FROM enriched
GROUP BY account_account_type
HAVING num_rows >= 1
ORDER BY num_rows DESC;
