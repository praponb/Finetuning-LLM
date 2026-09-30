WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        INITCAP(TRIM(plan)) AS plan_title,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM subscriptions
    WHERE end_date > DATE '2023-08-08'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        plan_title,
        SUM(monthly_fee) OVER w_hist AS running_monthly_fee,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior,
        CUME_DIST() OVER w_ordered AS running_monthly_fee_cume_dist
    FROM enriched
    WHERE YEAR(end_date) >= 2025
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY running_monthly_fee DESC)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        plan_title,
        monthly_fee_bucket,
        monthly_fee_avg_5_prior,
        running_monthly_fee_cume_dist,
        DATE_ADD(start_date, 44) AS start_date_plus_44d
    FROM enriched_subscriptions
    WHERE start_date > DATE '2023-08-17'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(monthly_fee_avg_5_prior, 2) OVER (ORDER BY running_monthly_fee_cume_dist, id) AS next_monthly_fee_avg_5_prior,
        AVG(monthly_fee_avg_5_prior) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior_avg_5_prior,
        LAST_VALUE(monthly_fee_bucket, TRUE) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date) ORDER BY running_monthly_fee_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_bucket
    FROM valid
),
totals AS (
    -- Grouped metrics
    SELECT
        customer_id,
        DATE_TRUNC('MONTH', end_date) AS activity_month,
        DATE_TRUNC('QUARTER', start_date) AS quarter_start,
        MAX(monthly_fee_avg_5_prior_avg_5_prior) AS max_monthly_fee_avg_5_prior_avg_5_prior,
        COUNT(*) AS cnt
    FROM scored
    GROUP BY customer_id, activity_month, quarter_start
)
SELECT
    customer_id,
    activity_month,
    quarter_start,
    max_monthly_fee_avg_5_prior_avg_5_prior
FROM totals;
