WITH filtered AS (
    SELECT
        *,
        ABS(balance) AS abs_balance,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM accounts
    WHERE opened_date > DATE '2023-09-17'
      AND customer_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY abs_balance DESC NULLS LAST, id) = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY abs_balance NULLS FIRST, id) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.abs_balance,
        c.age
    FROM deduped s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
deduped_accounts AS (
    SELECT
        id,
        customer_id,
        balance,
        age
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY age DESC NULLS LAST, id) AS dedup_rank
        FROM with_dims
    ) t
    WHERE dedup_rank = 1
),
daily AS (
    SELECT
        customer_id,
        CASE WHEN balance >= 85000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        STDDEV_SAMP(balance) AS balance_stddev,
        ROUND(AVG(balance), 2) AS avg_balance,
        MIN(balance) AS min_balance,
        SUM(age) AS total_age,
        PERCENTILE_APPROX(age, 0.95) AS age_p95,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY SUM(age) DESC) AS total_age_percent_rank
    FROM deduped_accounts
    GROUP BY customer_id, CASE WHEN balance >= 85000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    s.customer_id,
    s.balance_tier,
    s.balance_stddev,
    s.min_balance,
    s.total_age,
    s.total_age_percent_rank,
    (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_amount
FROM daily s
ORDER BY balance_tier DESC;
