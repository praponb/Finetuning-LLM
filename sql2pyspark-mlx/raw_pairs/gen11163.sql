WITH base AS (
    SELECT
        *,
        CASE WHEN balance >= 75000 THEN 'High' WHEN balance >= 37000 THEN 'Medium' ELSE 'Low' END AS balance_band,
        CASE WHEN account_type IN ('CHECKING', 'CREDIT') THEN 'Tier 1' ELSE 'Other' END AS account_type_group,
        COALESCE(status, 'NONE') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-08-20'
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
joined AS (
    SELECT
        e.*,
        c.segment,
        c.age,
        c.credit_limit
    FROM not_flagged e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
scored AS (
    SELECT
        *,
        LAG(credit_limit) OVER (PARTITION BY customer_id ORDER BY age, id) AS prev_credit_limit,
        LEAD(credit_limit) OVER (PARTITION BY customer_id ORDER BY age, id) AS next_credit_limit,
        LEAD(balance) OVER (PARTITION BY customer_id ORDER BY age, id) AS next_balance,
        LAG(balance) OVER (PARTITION BY customer_id ORDER BY age, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY age, id) AS row_seq
    FROM joined
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY age NULLS FIRST, id) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
remaining_keys AS (
    SELECT status, id
    FROM deduped
    EXCEPT
    SELECT status, id
    FROM accounts
    WHERE balance < 31000
),
verified AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
)
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    customer_id,
    MAX(balance) FILTER (WHERE status = 'FROZEN') AS frozen_max_balance,
    COUNT(DISTINCT segment) AS distinct_segments,
    SUM(CASE WHEN segment = 'Corporate' THEN next_balance ELSE 0 END) AS corporate_next_balance,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    GROUPING_ID() AS grouping_level
FROM verified
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', opened_date), customer_id)
ORDER BY distinct_segments NULLS FIRST;
