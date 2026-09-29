WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        MONTH(start_date) AS start_date_month,
        ROUND(monthly_fee, 2) AS monthly_fee_rounded
    FROM subscriptions
    WHERE YEAR(end_date) = 2023
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(monthly_fee, 1) OVER w_ordered AS next_monthly_fee,
        AVG(start_date_month) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee_rounded, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS start_date_month_avg_2_prior,
        LAST_VALUE(monthly_fee_rounded) IGNORE NULLS OVER w_recent AS last_known_monthly_fee_rounded
    FROM base
    WHERE customer_id IS NOT NULL
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY monthly_fee, id), w_recent AS (PARTITION BY customer_id ORDER BY start_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(end_date, start_date) / 7 AS weeks_start_date_to_end_date,
        monthly_fee / NULLIF(start_date_month_avg_2_prior, 0) AS monthly_fee_per_start_date_month_avg_2_prior
    FROM with_windows
    WHERE end_date >= DATE '2023-10-27'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY YEAR(end_date) ORDER BY weeks_start_date_to_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date_month_avg_2_prior, id) AS event_seq,
        LAST_VALUE(weeks_start_date_to_end_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date_month_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weeks_start_date_to_end_date
    FROM valid
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY monthly_fee_rounded) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM deduped
    EXCEPT ALL
    SELECT customer_id, status
    FROM subscriptions
    WHERE monthly_fee <= 130
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
valid_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        start_date_month,
        monthly_fee_rounded,
        next_monthly_fee,
        start_date_month_avg_2_prior,
        last_known_monthly_fee_rounded,
        weeks_start_date_to_end_date,
        monthly_fee_per_start_date_month_avg_2_prior,
        event_seq,
        last_known_weeks_start_date_to_end_date,
        dedup_rank,
        TRIM(status) AS clean_status
    FROM verified
    WHERE start_date > DATE '2023-08-11'
      AND customer_id IS NOT NULL
)
-- Grouped metrics
SELECT
    status,
    plan,
    SUM(monthly_fee_rounded) FILTER (WHERE status = 'EXPIRED') AS expired_total_monthly_fee_rounded,
    ROUND(SUM(next_monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS next_monthly_fee_per_premium,
    SIZE(COLLECT_SET(status)) AS num_status
FROM valid_subscriptions
GROUP BY status, plan
HAVING SIZE(COLLECT_SET(status)) >= 1;
