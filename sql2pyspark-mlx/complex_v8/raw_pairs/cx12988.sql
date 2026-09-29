WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj,
        INITCAP(TRIM(status)) AS status_clean,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM policies
    WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_end_date
    FROM scoped s
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(first_policy_end_date, LAG(first_policy_end_date) OVER (PARTITION BY holder_id, status ORDER BY end_date_week, id)) AS days_since_prev_first_policy_end_date,
        LEAD(premium, 3, 0) OVER w_holder_id_status_by_end_date AS next3_premium,
        RANK() OVER w_all_by_start_date_ties AS start_date_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, status ORDER BY end_date_week, id), 3) AS end_date_week_percent_rank
    FROM profiled
    WINDOW w_holder_id_status_by_end_date AS (PARTITION BY holder_id, status ORDER BY end_date_week, id), w_all_by_start_date_ties AS (ORDER BY start_date DESC)
),
with_details_holder AS (
    -- Join policyholders
    SELECT s.*, p.joined_on AS holder_joined_on, p.holder_name
    FROM with_windows s
    JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'G%'
)
-- Subtotals with CUBE
SELECT
    product,
    status,
    start_date_rank,
    COUNT(DISTINCT status) AS distinct_status
FROM with_details_holder
GROUP BY CUBE(product, status, start_date_rank)
ORDER BY product NULLS LAST, status NULLS LAST, start_date_rank NULLS LAST
