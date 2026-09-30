WITH enriched AS (
    SELECT
        *,
        LAG(monthly_fee, 1, 0) OVER (PARTITION BY plan ORDER BY monthly_fee, id) prev_monthly_fee,
        NTILE(10) OVER (PARTITION BY status ORDER BY monthly_fee, id) monthly_fee_bucket
    FROM subscriptions
),
with_windows AS (
    SELECT
        *,
        LEAD(monthly_fee_bucket, 2) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee, id) next_monthly_fee_bucket,
        AVG(monthly_fee) OVER (PARTITION BY customer_id, status) customer_id_avg_monthly_fee,
        LEAD(prev_monthly_fee, 1, 0) OVER (PARTITION BY plan ORDER BY monthly_fee, id) next_prev_monthly_fee
    FROM enriched
),
enriched_subscriptions AS (
    SELECT
        a.*,
        d.segment,
        d.signup_date
    FROM with_windows a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
enriched_subscriptions_2 AS (
    SELECT
        *,
        COUNT(customer_id_avg_monthly_fee) OVER (PARTITION BY customer_id, plan) customer_id_count_customer_id_avg_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) monthly_fee_share,
        SUM(next_monthly_fee_bucket) OVER (PARTITION BY customer_id, status ORDER BY prev_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_monthly_fee_bucket
    FROM enriched_subscriptions
)
SELECT
    segment,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    SUM(CASE WHEN plan = 'standard' THEN monthly_fee_share ELSE 0 END) AS standard_monthly_fee_share,
    SUM(CASE WHEN plan = 'basic' THEN next_prev_monthly_fee ELSE 0 END) AS basic_next_prev_monthly_fee
FROM enriched_subscriptions_2
GROUP BY 1
ORDER BY num_plans
