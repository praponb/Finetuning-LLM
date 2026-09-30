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
        LAST_DAY(end_date) AS end_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        end_date_month_end,
        DATE_FORMAT(end_date_month_end, 'MMM yyyy') AS end_date_month_end_label
    FROM cleaned
    WHERE end_date >= TO_DATE('2023-06-30')
      AND plan = 'standard'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        COUNT(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_monthly_fee,
        MAX(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_monthly_fee
    FROM valid
    WHERE start_date < '2023-08-29'
),
cleaned_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        MONTH(start_date) AS start_date_month,
        CASE WHEN running_count_monthly_fee >= 13 THEN 'High' WHEN running_count_monthly_fee >= 3 THEN 'Medium' ELSE 'Low' END AS running_count_monthly_fee_tier
    FROM enriched
    WHERE end_date > DATE '2023-08-04'
      AND customer_id IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned_subscriptions src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date >= sub.start_date
       AND src.end_date < sub.end_date + INTERVAL '14' DAY
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        running_count_monthly_fee,
        running_max_monthly_fee,
        running_count_monthly_fee_tier,
        subscription_id,
        monthly_fee,
        AVG(running_count_monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date, id, subscription_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_count_monthly_fee_avg_3_prior,
        SUM(running_max_monthly_fee) OVER (PARTITION BY plan ORDER BY start_date_month, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_monthly_fee,
        NTILE(4) OVER (PARTITION BY status ORDER BY running_count_monthly_fee, id, subscription_id) AS running_count_monthly_fee_bucket,
        MIN(running_count_monthly_fee) OVER (PARTITION BY plan) AS plan_min_running_count_monthly_fee,
        AVG(running_min_monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY running_count_monthly_fee, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_min_monthly_fee_avg_3
    FROM with_window
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date NULLS LAST, id, subscription_id) AS dedup_rank
        FROM enriched_subscriptions
    ) ranked
    WHERE dedup_rank = 1
)
-- Join a dimension and aggregate
SELECT
    d.city,
    d.segment,
    COUNT(*) AS cnt,
    MAX(src.plan_min_running_count_monthly_fee) AS max_plan_min_running_count_monthly_fee,
    ROUND(AVG(src.running_running_max_monthly_fee), 2) AS avg_running_running_max_monthly_fee
FROM latest src
LEFT JOIN customers d ON src.customer_id = d.id
GROUP BY d.city, d.segment;
