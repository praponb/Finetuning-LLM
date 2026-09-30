WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DAY(end_date) AS end_date_day
    FROM subscriptions
    WHERE start_date >= DATE '2023-03-18'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(end_date_day) OVER w_part AS last4_sum_end_date_day,
        COUNT(end_date_day) OVER w_part AS last4_count_end_date_day,
        MIN(end_date_day) OVER w_part AS last4_min_end_date_day,
        STDDEV_SAMP(end_date_day) OVER w_ordered AS end_date_period_stddev_samp_end_date_day
    FROM valid
    WHERE end_date BETWEEN '2024-02-01' AND '2025-05-09'
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY DATE_TRUNC('WEEK', end_date), status)
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.signup_date,
        d.city
    FROM metrics e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY end_date_period_stddev_samp_end_date_day DESC NULLS FIRST) AS end_date_period_stddev_samp_end_date_day_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY start_date DESC, id ASC) AS row_seq,
        AVG(monthly_fee) OVER (PARTITION BY DATE_TRUNC('month', end_date) ORDER BY end_date_period_stddev_samp_end_date_day ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6,
        LEAD(last4_sum_end_date_day, 1, 0) OVER (PARTITION BY status, customer_id ORDER BY last4_sum_end_date_day ASC, id ASC) AS next_last4_sum_end_date_day,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY signup_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6_2
    FROM with_dims
)
SELECT
    id,
    customer_id,
    plan,
    status,
    end_date_day,
    last4_sum_end_date_day,
    last4_count_end_date_day,
    last4_min_end_date_day,
    end_date_period_stddev_samp_end_date_day
FROM with_windows
