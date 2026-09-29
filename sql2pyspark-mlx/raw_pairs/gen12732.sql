WITH joined AS (
    SELECT src.*, b.name
    FROM subscriptions src
    JOIN customers b
        ON src.customer_id = b.id
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(monthly_fee, TRUE) OVER w_grp AS last_known_monthly_fee,
        SUM(monthly_fee) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior
    FROM joined
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT
        *,
        monthly_fee_avg_2_prior + last_known_monthly_fee AS monthly_fee_avg_2_prior_plus_last_known_monthly_fee,
        LOWER(status) AS status_lower
    FROM with_windows
    WHERE start_date >= TO_DATE('2023-05-10')
),
joined_subscriptions AS (
    SELECT src.*, b.signup_date
    FROM filtered src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.credit_limit >= 9500
)
SELECT
    c.segment,
    e.plan,
    COUNT(*) AS num_rows,
    MAX(e.monthly_fee) AS max_monthly_fee
FROM joined_subscriptions e
JOIN customers c ON e.customer_id = c.id
GROUP BY c.segment, e.plan
ORDER BY plan DESC NULLS LAST
