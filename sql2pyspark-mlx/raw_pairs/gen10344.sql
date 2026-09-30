WITH ranked AS (
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST, id) = 1
),
deduped AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS FIRST, id) = 1
),
filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date,
        DATE_SUB(end_date, 57) AS end_date_minus_57d
    FROM deduped
    WHERE customer_id IS NOT NULL
      AND end_date BETWEEN '2023-06-01' AND '2024-01-01'
),
base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date_minus_57d) / 30 AS months_since_end_date_minus_57d
    FROM filtered
    WHERE days_since_end_date < 53
),
scored AS (
    SELECT
        id,
        plan,
        end_date,
        status,
        months_since_end_date_minus_57d,
        MAX(days_since_end_date) OVER (PARTITION BY status) AS status_max_days_since_end_date,
        AVG(months_since_end_date_minus_57d) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS months_since_end_date_minus_57d_avg_5_prior,
        FIRST_VALUE(days_since_end_date) IGNORE NULLS OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_end_date
    FROM base
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), plan ORDER BY months_since_end_date_minus_57d_avg_5_prior DESC, id) AS months_since_end_date_minus_57d_avg_5_prior_bucket,
        LAG(months_since_end_date_minus_57d, 2, 0) OVER (PARTITION BY status ORDER BY first_known_days_since_end_date, id) AS prev_months_since_end_date_minus_57d
    FROM scored
    WHERE end_date BETWEEN '2023-11-01' AND '2024-03-01'
)
SELECT
    plan,
    COUNT_IF(status = 'PAUSED') AS paused_events,
    SIZE(COLLECT_SET(status)) AS num_status
FROM with_windows
GROUP BY plan;
