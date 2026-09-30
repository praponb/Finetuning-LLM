WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CONCAT(account_type, '/', status) AS account_type_status
    FROM accounts
    WHERE status IN ('CLOSED', 'OPEN', 'FROZEN')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        account_type_status,
        AVG(balance) OVER (PARTITION BY status ORDER BY opened_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        SUM(balance) OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date) ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
    FROM valid
    WHERE status IN ('FROZEN', 'OPEN')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT account_type, id
    FROM enriched
    INTERSECT
    SELECT account_type, id
    FROM accounts
    WHERE customer_id IN (3, 7, 36)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.account_type = k.account_type AND s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        opened_date,
        balance_avg_2_prior,
        cumulative_balance,
        CONCAT_WS('/', status, account_type) AS status_account_type_key,
        CONCAT_WS(',', status, account_type) AS status_account_type_key_2,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM verified
    WHERE status IS NOT DISTINCT FROM 'FROZEN'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.balance_avg_2_prior,
        a.cumulative_balance,
        d.age,
        d.signup_date,
        d.name
    FROM filtered a
    INNER JOIN customers d
        ON a.customer_id = d.id
)
SELECT
    s.id,
    s.customer_id,
    s.balance_avg_2_prior,
    s.cumulative_balance,
    s.name,
    CASE WHEN name RLIKE '^(Noah Park|Emma Brown)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS name_zone,
    (SELECT AVG(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM joined s
