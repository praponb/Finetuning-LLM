WITH metrics AS (
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_3,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        NTILE(4) OVER (ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM subscriptions
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_avg_2_prior DESC NULLS FIRST) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
),
scored AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_avg_3,
        monthly_fee_avg_2_prior,
        running_monthly_fee,
        monthly_fee_bucket,
        latest_rank,
        ROUND(monthly_fee_avg_2_prior / SUM(monthly_fee_avg_2_prior) OVER (PARTITION BY customer_id, plan), 4) AS monthly_fee_avg_2_prior_share,
        SUM(running_monthly_fee) OVER (PARTITION BY status) AS status_sum_running_monthly_fee
    FROM deduped
),
kept_keys AS (
    SELECT customer_id, status
    FROM scored
    INTERSECT
    SELECT customer_id, status
    FROM subscriptions
    WHERE status NOT IN ('PAUSED', 'CANCELLED')
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
)
SELECT
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    SUM(monthly_fee_bucket) FILTER (WHERE plan = 'basic') AS basic_total_monthly_fee_bucket,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
FROM verified
GROUP BY DATE_TRUNC('QUARTER', start_date)
ORDER BY basic_total_monthly_fee_bucket DESC
