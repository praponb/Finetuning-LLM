WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        DATEDIFF(CURRENT_DATE(), start_date) / 365 AS years_since_start_date,
        CASE WHEN status IN ('ACTIVE', 'PAUSED') THEN 'Tier 1' ELSE 'Other' END AS status_group
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-24'
      AND customer_id IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, monthly_fee * years_since_start_date AS monthly_fee_times_years_since_start_date
    FROM filtered
    WHERE end_date > DATE '2023-08-26'
      AND customer_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        years_since_start_date,
        status_group,
        monthly_fee_times_years_since_start_date,
        monthly_fee_times_years_since_start_date + years_since_start_date AS monthly_fee_times_years_since_start_date_plus_years_since_start_date
    FROM valid
    WHERE customer_id IS NOT NULL
      AND years_since_start_date < 65
),
valid_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status_group,
        monthly_fee_times_years_since_start_date,
        monthly_fee_times_years_since_start_date_plus_years_since_start_date,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM cleaned
    WHERE end_date > '2024-07-01'
      AND end_date > DATE '2023-07-11'
),
cleaned_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        monthly_fee_times_years_since_start_date,
        monthly_fee_times_years_since_start_date_plus_years_since_start_date,
        monthly_fee_int,
        CAST(monthly_fee_times_years_since_start_date AS INT) AS monthly_fee_times_years_since_start_date_int
    FROM valid_subscriptions
    WHERE end_date > DATE '2023-06-05'
      AND customer_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_times_years_since_start_date_plus_years_since_start_date DESC NULLS FIRST, id) AS rn
    FROM cleaned_subscriptions
    QUALIFY rn = 1
)
SELECT
    id,
    customer_id,
    monthly_fee_times_years_since_start_date,
    monthly_fee_int,
    monthly_fee_times_years_since_start_date_int
FROM latest
ORDER BY monthly_fee_times_years_since_start_date_int
