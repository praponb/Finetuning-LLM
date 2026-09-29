WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DAY(end_date) AS end_date_day,
        ROUND(DATEDIFF(end_date, start_date) / 30, 1) AS months_start_date_to_end_date
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-06-17')
      AND plan NOT IN ('standard', 'enterprise', 'premium')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        start_date,
        status,
        SUM(months_start_date_to_end_date) OVER (PARTITION BY customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_months_start_date_to_end_date,
        NTILE(10) OVER w_grp AS months_start_date_to_end_date_bucket,
        FIRST_VALUE(months_start_date_to_end_date, TRUE) OVER (PARTITION BY YEAR(start_date), customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_months_start_date_to_end_date
    FROM valid
    WINDOW w_grp AS (PARTITION BY plan ORDER BY months_start_date_to_end_date ASC, id ASC)
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY months_start_date_to_end_date_bucket ASC, id ASC) AS months_start_date_to_end_date_bucket_bucket,
        FIRST_VALUE(first_known_months_start_date_to_end_date) IGNORE NULLS OVER (ORDER BY first_known_months_start_date_to_end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_months_start_date_to_end_date
    FROM scored
    WHERE months_start_date_to_end_date_bucket >= 7
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY months_start_date_to_end_date_bucket DESC, id ASC) AS dedup_rank
        FROM scored_subscriptions
    ) t
    WHERE dedup_rank = 1
),
latest_subscriptions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        start_date,
        status,
        cumulative_months_start_date_to_end_date,
        months_start_date_to_end_date_bucket_bucket,
        dedup_rank
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY customer_id, status ORDER BY start_date DESC NULLS LAST) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_months_start_date_to_end_date >= 375 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_months_start_date_to_end_date_tier,
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    ROUND(AVG(cumulative_months_start_date_to_end_date), 2) AS avg_cumulative_months_start_date_to_end_date,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count,
    COUNT(*) FILTER (WHERE status = 'PAUSED') AS paused_rows,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN cumulative_months_start_date_to_end_date >= 375 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) FILTER (WHERE status = 'PAUSED') DESC) AS paused_rows_percent_rank
FROM latest_subscriptions
GROUP BY CASE WHEN cumulative_months_start_date_to_end_date >= 375 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', start_date)
