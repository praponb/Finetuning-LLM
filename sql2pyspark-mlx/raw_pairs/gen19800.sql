WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date_2,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-08-24'
      AND status IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        resolved_status
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT account_type, customer_id
    FROM deduped
    INTERSECT
    SELECT account_type, customer_id
    FROM accounts
    WHERE customer_id IN (14, 16, 24)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.account_type = k.account_type AND s.customer_id = k.customer_id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        RANK() OVER w_grp AS balance_rank,
        AVG(balance) OVER (PARTITION BY customer_id) AS customer_id_avg_balance,
        SUM(balance) OVER (PARTITION BY status) AS status_sum_balance
    FROM verified
    WINDOW w_grp AS (ORDER BY balance NULLS FIRST)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance_rank,
        customer_id_avg_balance,
        status_sum_balance,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY status_sum_balance DESC NULLS LAST) AS status_sum_balance_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance_rank NULLS FIRST) AS balance_rank_percent_rank
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    account_type,
    CASE WHEN status_sum_balance_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS status_sum_balance_percent_rank_tier,
    CASE WHEN customer_id_avg_balance >= 86891 THEN 'LARGE' ELSE 'SMALL' END AS customer_id_avg_balance_tier,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN account_type = 'CREDIT' THEN customer_id_avg_balance ELSE 0 END) AS credit_customer_id_avg_balance,
    COUNT(*) FILTER (WHERE account_type = 'CREDIT') AS credit_rows,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count,
    AVG(status_sum_balance) AS avg_status_sum_balance,
    ROUND(SUM(balance_rank) / NULLIF(COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END), 0), 2) AS balance_rank_per_savings
FROM with_windows
GROUP BY account_type, status_sum_balance_percent_rank_tier, customer_id_avg_balance_tier
HAVING credit_rows >= 1;
