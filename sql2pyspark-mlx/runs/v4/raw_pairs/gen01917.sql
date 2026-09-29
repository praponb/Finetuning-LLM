WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LAG(balance, 2) OVER (PARTITION BY account_type, customer_id ORDER BY balance ASC, id ASC) AS prev_balance,
        SUM(balance) OVER (ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY balance ASC NULLS FIRST) AS balance_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC) AS row_seq,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY account_type, status ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status
    FROM accounts
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.city,
        b.name
    FROM not_flagged s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.name AS customer_name,
        d.credit_limit,
        d.country
    FROM joined s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, account_type
    FROM with_dims
    EXCEPT DISTINCT
    SELECT id, account_type
    FROM accounts
    WHERE balance >= 19000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN kept_keys k ON s.id = k.id AND s.account_type = k.account_type
),
joined_accounts AS (
    -- Attach customers attributes
    SELECT src.*, p.name AS customer_name_2
    FROM reconciled src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        prev_balance,
        cumulative_balance,
        balance_cume_dist,
        row_seq,
        city,
        name,
        customer_name,
        credit_limit,
        country,
        SUBSTRING(name, 1, 4) AS name_prefix,
        ROUND(balance_cume_dist * 1.07, 2) AS balance_cume_dist_scaled
    FROM joined_accounts
    WHERE country IS NOT NULL
      AND prev_balance >= 39000
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN balance_cume_dist_scaled >= 75 THEN 'HIGH' ELSE 'LOW' END AS balance_cume_dist_scaled_tier,
        CASE WHEN prev_balance >= 88000 THEN 'HIGH' ELSE 'LOW' END AS prev_balance_tier,
        city,
        MAX(balance_cume_dist_scaled) AS max_balance_cume_dist_scaled,
        SUM(CASE WHEN account_type = 'SAVINGS' THEN prev_balance ELSE 0 END) AS savings_prev_balance,
        ROUND(SUM(prev_balance) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS prev_balance_per_bangkok,
        GROUPING(CASE WHEN balance_cume_dist_scaled >= 75 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM cleaned
    GROUP BY ROLLUP (CASE WHEN balance_cume_dist_scaled >= 75 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_balance >= 88000 THEN 'HIGH' ELSE 'LOW' END, city)
)
SELECT
    balance_cume_dist_scaled_tier,
    prev_balance_tier,
    city,
    max_balance_cume_dist_scaled,
    savings_prev_balance
FROM totals;
