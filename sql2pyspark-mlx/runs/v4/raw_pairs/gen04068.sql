WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE end_date >= DATE '2024-01-31'
      AND customer_id IS NOT NULL
),
filtered_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        end_date,
        monthly_fee_whole,
        ROUND(monthly_fee_whole * 100, 2) AS monthly_fee_whole_scaled
    FROM filtered
    WHERE end_date > DATE '2023-06-06'
      AND customer_id IS NOT NULL
      AND plan IN ('enterprise', 'basic', 'premium')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        monthly_fee_whole,
        monthly_fee_whole_scaled,
        NTILE(4) OVER (ORDER BY monthly_fee_whole_scaled, id) AS monthly_fee_whole_scaled_bucket,
        LAST_VALUE(monthly_fee_whole_scaled, TRUE) OVER (ORDER BY monthly_fee_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_whole_scaled,
        AVG(monthly_fee_whole) OVER (ORDER BY monthly_fee_whole, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_whole_avg_5_prior
    FROM filtered_subscriptions
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, monthly_fee_whole_avg_5_prior * monthly_fee_whole_scaled_bucket AS monthly_fee_whole_avg_5_prior_times_monthly_fee_whole_scaled_bucket
    FROM metrics
    WHERE end_date >= TO_DATE('2023-06-02')
)
SELECT
    id,
    end_date,
    monthly_fee_whole,
    monthly_fee_whole_scaled_bucket,
    monthly_fee_whole_avg_5_prior,
    monthly_fee_whole_avg_5_prior_times_monthly_fee_whole_scaled_bucket
FROM cleaned
ORDER BY monthly_fee_whole_scaled_bucket DESC NULLS FIRST;
