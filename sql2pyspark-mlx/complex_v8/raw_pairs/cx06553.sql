WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM policies
    WHERE (product IN ('TRAVEL', 'LIFE', 'HOME') OR holder_id >= 5)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_name, p.joined_on AS holder_joined_on
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        NTILE(3) OVER w_all_by_premium AS premium_tercile,
        NTH_VALUE(end_date, 2) OVER (ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        DENSE_RANK() OVER (ORDER BY premium NULLS LAST, id) AS premium_dense_rank,
        MIN(premium) OVER (ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(AVG(premium) OVER (PARTITION BY status), 2) AS status_avg_premium,
        ROUND(PERCENT_RANK() OVER w_all_by_holder_joined_on_ties, 3) AS holder_joined_on_percent_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        LEAD(end_date) OVER w_all_by_holder_joined_on AS next_end_date,
        ROW_NUMBER() OVER (ORDER BY premium NULLS LAST, id) AS rn
    FROM with_parent
    WINDOW w_all_by_premium AS (ORDER BY premium NULLS LAST, id), w_all_by_holder_joined_on_ties AS (ORDER BY holder_joined_on DESC), w_all_by_holder_joined_on AS (ORDER BY holder_joined_on DESC, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(end_date, 2) OVER (PARTITION BY premium_tercile, rn ORDER BY premium_share_of_status, id) AS prev2_end_date,
        SUM(status_avg_premium) OVER w_start_date_month AS start_date_month_sum_status_avg_premium,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS rows_in_start_date_month,
        premium_vs_status_avg - LAG(premium_vs_status_avg) OVER (PARTITION BY premium_tercile, rn ORDER BY premium_share_of_status, id) AS premium_vs_status_avg_change
    FROM windowed
    WINDOW w_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', start_date))
)
-- Final projection
SELECT
    id,
    product,
    premium,
    end_date,
    status,
    end_date_label,
    holder_name,
    holder_joined_on,
    rows_in_status,
    premium_tercile,
    premium_dense_rank,
    min_premium_so_far,
    status_avg_premium,
    holder_joined_on_percent_rank,
    premium_share_of_status,
    premium_vs_status_avg,
    next_end_date,
    rn,
    prev2_end_date,
    start_date_month_sum_status_avg_premium,
    rows_in_start_date_month
FROM sequenced
WHERE start_date_month_sum_status_avg_premium BETWEEN 10160.0 AND 37550.0
ORDER BY id DESC NULLS FIRST, premium_tercile DESC
