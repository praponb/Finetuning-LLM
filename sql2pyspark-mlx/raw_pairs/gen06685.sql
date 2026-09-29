WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date) / 30 AS months_since_end_date
    FROM subscriptions
    WHERE end_date < '2023-07-12'
      AND customer_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.plan,
        e.monthly_fee,
        b.name,
        b.credit_limit
    FROM valid e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        monthly_fee,
        credit_limit,
        MIN(credit_limit) OVER w_ordered AS running_min_credit_limit,
        AVG(credit_limit) OVER w_ordered AS running_avg_credit_limit,
        COUNT(credit_limit) OVER w_ordered AS running_count_credit_limit
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        monthly_fee,
        credit_limit,
        running_min_credit_limit,
        running_avg_credit_limit,
        ROUND(running_avg_credit_limit, 0) AS running_avg_credit_limit_rounded
    FROM metrics
    WHERE running_count_credit_limit != 15
)
SELECT
    id,
    credit_limit,
    running_min_credit_limit,
    running_avg_credit_limit
FROM filtered
ORDER BY credit_limit DESC, id
