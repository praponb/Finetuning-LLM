WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(opened_date, 1) AS opened_date_plus_months,
        CONCAT(account_type, ' ', status) AS account_type_status,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE status IN ('FROZEN', 'CLOSED')
      AND opened_date >= TO_DATE('2023-07-03')
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date_plus_months
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT status
    FROM not_flagged
    EXCEPT DISTINCT
    SELECT status
    FROM accounts
    WHERE customer_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN kept_keys k ON s.status = k.status
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.balance,
        src.opened_date,
        d.age
    FROM eligible src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.balance,
        s.age,
        c.segment,
        c.age AS customer_age,
        c.city
    FROM joined s
    LEFT JOIN customers c
        ON s.customer_id <=> c.id
)
SELECT
    s.customer_id,
    s.age,
    s.segment,
    s.customer_age,
    s.city,
    CASE WHEN city RLIKE '^(Singapore|Tokyo)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM with_dims s;
