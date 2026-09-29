WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(plan) AS clean_plan,
        LENGTH(status) AS status_length
    FROM subscriptions
    WHERE end_date > DATE '2023-07-29'
      AND customer_id = 5
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date ASC NULLS LAST, id ASC) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
base_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        end_date,
        clean_plan,
        status_length,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM latest
    WHERE start_date > DATE '2023-04-23'
      AND monthly_fee BETWEEN 110 AND 230
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        end_date,
        clean_plan,
        RANK() OVER (PARTITION BY YEAR(end_date) ORDER BY days_since_end_date DESC) AS days_since_end_date_rank,
        AVG(days_since_end_date) OVER (PARTITION BY plan ORDER BY days_since_end_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_end_date_avg_3_prior
    FROM base_subscriptions
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT ALL
    SELECT id
    FROM subscriptions
    WHERE start_date BETWEEN '2024-01-01' AND '2024-07-19'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        start_date,
        clean_plan,
        days_since_end_date_rank,
        days_since_end_date_avg_3_prior,
        PERCENT_RANK() OVER (ORDER BY days_since_end_date_rank ASC NULLS FIRST) AS days_since_end_date_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY days_since_end_date_rank DESC) AS days_since_end_date_rank_cume_dist
    FROM kept_rows
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    APPROX_PERCENTILE(days_since_end_date_rank_percent_rank, 0.9) AS days_since_end_date_rank_percent_rank_p90,
    MAX(days_since_end_date_rank) AS max_days_since_end_date_rank,
    APPROX_PERCENTILE(days_since_end_date_rank, 0.9) AS days_since_end_date_rank_p90
FROM with_windows
GROUP BY 1
HAVING days_since_end_date_rank_percent_rank_p90 > 0
ORDER BY year_start ASC NULLS LAST;
