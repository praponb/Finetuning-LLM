WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        CONCAT_WS(': ', product, status) AS product_status_label,
        INITCAP(TRIM(product)) AS product_clean
    FROM policies
    WHERE (premium >= 1042.5 OR start_date < DATE '2025-02-14')
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM scoped_policys
    GROUP BY status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM scoped_policys s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        start_date,
        product_status_label,
        product_clean,
        premium_minus_avg,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        LAST_VALUE(premium_minus_avg) OVER (PARTITION BY status ORDER BY premium_minus_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_minus_avg,
        ROUND(premium / NULLIF(SUM(premium) OVER w_status, 0), 4) AS premium_share_of_status,
        ROUND(PERCENT_RANK() OVER w_status_by_premium_minus_avg_2, 3) AS premium_minus_avg_percent_rank
    FROM vs_group
    WINDOW w_status AS (PARTITION BY status), w_status_by_premium_minus_avg_2 AS (PARTITION BY status ORDER BY premium_minus_avg DESC, id)
),
latest AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        start_date,
        product_status_label,
        product_clean,
        premium_minus_avg,
        rows_in_status,
        last_premium_minus_avg,
        premium_share_of_status,
        premium_minus_avg_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium_minus_avg DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(start_date), holder_id) AS rows_in_start_date_month_holder_id,
        MAX(premium_minus_avg) OVER w_start_date_month_holder_id AS start_date_month_holder_id_max_premium_minus_avg,
        LAG(premium_share_of_status) OVER w_start_date_year_by_last_prem AS prev_premium_share_of_status,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id ORDER BY premium_minus_avg_percent_rank DESC, id)) AS days_since_prev_start_date
    FROM latest
    WINDOW w_start_date_month_holder_id AS (PARTITION BY MONTH(start_date), holder_id), w_start_date_year_by_last_prem AS (PARTITION BY YEAR(start_date) ORDER BY last_premium_minus_avg ASC, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        holder_id,
        COUNT(*) AS row_count,
        MIN(start_date) AS first_start_date
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', start_date), holder_id
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        start_date_month,
        holder_id,
        first_start_date,
        NTILE(5) OVER w_row_count_first_start_date_y AS row_count_quintile,
        ROUND(PERCENT_RANK() OVER w_row_count_first_start_date_y_2, 3) AS row_count_percent_rank,
        ROUND(AVG(row_count) OVER (PARTITION BY DATE_TRUNC('MONTH', first_start_date)), 2) AS first_start_date_month_avg_row_count
    FROM rollup_base
    WINDOW w_row_count_first_start_date_y AS (PARTITION BY row_count, YEAR(first_start_date) ORDER BY row_count DESC, start_date_month, holder_id), w_row_count_first_start_date_y_2 AS (PARTITION BY row_count, YEAR(first_start_date) ORDER BY row_count DESC)
)
-- Final projection
SELECT DISTINCT
    row_count_percent_rank,
    first_start_date_month_avg_row_count,
    start_date_month
FROM sequenced
WHERE row_count_quintile < 8
ORDER BY first_start_date_month_avg_row_count, start_date_month DESC NULLS FIRST
