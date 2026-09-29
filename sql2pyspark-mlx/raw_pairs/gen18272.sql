WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(monthly_fee) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date), status) AS start_date_period_count_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY MONTH(start_date), status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        LEAD(monthly_fee, 1) OVER (PARTITION BY status ORDER BY end_date, id) AS next_monthly_fee
    FROM subscriptions
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(next_monthly_fee AS INT) AS next_monthly_fee_int,
        next_monthly_fee - start_date_period_count_monthly_fee AS next_monthly_fee_minus_start_date_period_count_monthly_fee
    FROM metrics
    WHERE start_date >= DATE '2023-06-14'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share,
        SUM(start_date_period_count_monthly_fee) OVER (PARTITION BY customer_id ORDER BY next_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_start_date_period_count_monthly_fee,
        SUM(cumulative_monthly_fee) OVER w_grp AS cumulative_cumulative_monthly_fee
    FROM filtered
    WHERE start_date_period_count_monthly_fee BETWEEN 9 AND 12
    WINDOW w_grp AS (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(next_monthly_fee_int) OVER w_hist AS running_avg_next_monthly_fee_int,
        MIN(next_monthly_fee_int) OVER w_hist AS running_min_next_monthly_fee_int,
        MAX(next_monthly_fee_int) OVER w_hist AS running_max_next_monthly_fee_int
    FROM scored
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY running_start_date_period_count_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        start_date_period_count_monthly_fee,
        next_monthly_fee,
        next_monthly_fee_int,
        monthly_fee_share,
        running_start_date_period_count_monthly_fee,
        running_avg_next_monthly_fee_int,
        running_min_next_monthly_fee_int,
        running_max_next_monthly_fee_int,
        SUM(start_date_period_count_monthly_fee) OVER (ORDER BY running_start_date_period_count_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_start_date_period_count_monthly_fee_2,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY cumulative_monthly_fee, id) AS cumulative_monthly_fee_bucket,
        STDDEV_SAMP(next_monthly_fee) OVER (PARTITION BY plan, status) AS plan_stddev_samp_next_monthly_fee,
        NTILE(5) OVER (PARTITION BY plan ORDER BY running_min_next_monthly_fee_int, id) AS running_min_next_monthly_fee_int_bucket,
        DENSE_RANK() OVER (ORDER BY monthly_fee_share DESC NULLS LAST) AS monthly_fee_share_dense_rank
    FROM metrics_subscriptions
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.monthly_fee,
    s.status,
    s.start_date_period_count_monthly_fee,
    s.next_monthly_fee_int,
    s.monthly_fee_share,
    s.running_start_date_period_count_monthly_fee,
    (SELECT MIN(t2.discount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_discount
FROM scored_subscriptions s;
