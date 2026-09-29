WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM subscriptions e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.end_date >= sub.start_date
       AND e.end_date <= DATE_ADD(sub.end_date, 3)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(subscription_id) OVER (PARTITION BY plan ORDER BY end_date, id, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS subscription_id_avg_5,
        LEAD(monthly_fee, 1) OVER w_part AS next_monthly_fee,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY status), 4) AS subscription_id_share
    FROM matched
    WINDOW w_part AS (PARTITION BY plan ORDER BY subscription_id, id, subscription_id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS subscription_id_share_dense_rank,
        ROUND(subscription_monthly_fee / SUM(subscription_monthly_fee) OVER (PARTITION BY status), 4) AS subscription_monthly_fee_share,
        AVG(subscription_id) OVER (PARTITION BY customer_id ORDER BY next_monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_2_prior
    FROM enriched
    WINDOW w_grp AS (PARTITION BY subscription_plan ORDER BY subscription_id_share NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN subscription_id_avg_2_prior >= 10 THEN 'HIGH' ELSE 'LOW' END AS subscription_id_avg_2_prior_tier,
    DATE_TRUNC('YEAR', end_date) AS year_start,
    CASE WHEN next_monthly_fee >= 230 THEN 'LARGE' ELSE 'SMALL' END AS next_monthly_fee_tier,
    PERCENTILE_APPROX(subscription_id_avg_5, 0.5) AS subscription_id_avg_5_p50,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    MAX(subscription_monthly_fee) FILTER (WHERE subscription_plan = 'enterprise') AS enterprise_max_subscription_monthly_fee
FROM with_windows
GROUP BY subscription_id_avg_2_prior_tier, year_start, next_monthly_fee_tier
