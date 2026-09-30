WITH with_last AS (
    SELECT
        s.id,
        s.name,
        s.country,
        s.segment,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.age,
        top1.subscription_end_date,
        top1.monthly_fee
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.end_date AS subscription_end_date, c.monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_customers AS (
    SELECT
        s.*,
        latest.status,
        latest.account_type
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.status, c.account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date < s.signup_date
        )
),
metrics AS (
    SELECT
        id,
        name,
        country,
        segment,
        signup_date,
        credit_limit,
        subscription_end_date,
        monthly_fee,
        account_type,
        LEAD(age, 2) OVER (PARTITION BY account_type ORDER BY signup_date, id) AS next_age,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', subscription_end_date), country ORDER BY monthly_fee DESC, id) AS row_seq,
        PERCENT_RANK() OVER (ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_percent_rank
    FROM not_flagged
)
SELECT
    CASE WHEN monthly_fee >= 212 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    PERCENTILE_APPROX(monthly_fee_percent_rank, 0.5) AS monthly_fee_percent_rank_p50,
    ROUND(AVG(monthly_fee_percent_rank), 2) AS avg_monthly_fee_percent_rank,
    MAX(monthly_fee) FILTER (WHERE country = 'UK') AS uk_max_monthly_fee
FROM metrics
GROUP BY 1
ORDER BY uk_max_monthly_fee;
