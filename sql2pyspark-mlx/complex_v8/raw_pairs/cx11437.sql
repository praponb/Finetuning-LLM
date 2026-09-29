WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE (status = 'ACTIVE' OR YEAR(end_date) = 2025)
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_filled,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        start_date,
        end_date,
        status,
        premium_filled,
        holder_region,
        holder_risk_score,
        COUNT(*) OVER w_status AS rows_in_status,
        LEAD(start_date) OVER (PARTITION BY MONTH(end_date) ORDER BY start_date DESC, id) AS next_start_date,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_id, holder_region), 0), 4) AS holder_risk_score_share_of_holder_id_holder_regi,
        NTILE(10) OVER (PARTITION BY holder_region ORDER BY holder_risk_score ASC, id) AS holder_risk_score_decile,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY holder_region ORDER BY holder_risk_score ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_2,
        ROUND(AVG(premium_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_filled_avg_6_prior,
        NTILE(5) OVER w_status_holder_id_by_end_date AS end_date_quintile,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY MONTH(end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_2,
        NTILE(5) OVER w_end_date_month_by_start_date_5 AS start_date_quintile,
        SUM(premium_filled) OVER (PARTITION BY holder_region ORDER BY holder_risk_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_filled,
        SUM(holder_risk_score) OVER w_holder_id_holder_region AS holder_id_holder_region_sum_holder_risk_score
    FROM joined_holder
    WINDOW w_status AS (PARTITION BY status), w_status_holder_id_by_end_date AS (PARTITION BY status, holder_id ORDER BY end_date DESC, id), w_end_date_month_by_start_date_5 AS (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date, id), w_holder_id_holder_region AS (PARTITION BY holder_id, holder_region)
),
tagged AS (
    -- Label and score rows
    SELECT *, CONCAT_WS('|', holder_region, status) AS holder_region_status_label
    FROM windowed
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        status,
        premium_filled,
        holder_region,
        next_start_date,
        holder_risk_score_decile,
        premium_filled_avg_6_prior,
        end_date_quintile,
        start_date_quintile,
        running_premium_filled,
        holder_region_status_label,
        RANK() OVER (ORDER BY running_premium_filled DESC, id) AS running_premium_filled_rank,
        LAG(rows_in_status) OVER (PARTITION BY start_date_quintile, end_date_quintile ORDER BY next_start_date DESC, id) AS prev_rows_in_status,
        DATEDIFF(end_date, LAG(end_date) OVER w_all_by_running_premium_fille) AS days_since_prev_end_date,
        ROUND(AVG(end_date_quintile) OVER (PARTITION BY DATE_TRUNC('MONTH', next_start_date)), 2) AS next_start_date_month_avg_end_date_quintile,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_next_start_date_month, 0), 4) AS holder_risk_score_share_of_next_start_date_month,
        ROUND(AVG(running_premium_filled) OVER w_next_start_date_month, 2) AS next_start_date_month_avg_running_premium_filled,
        FIRST_VALUE(running_premium_filled) OVER (PARTITION BY start_date_quintile, end_date_quintile ORDER BY next_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_running_premium_filled
    FROM tagged
    WINDOW w_all_by_running_premium_fille AS (ORDER BY running_premium_filled DESC, id), w_next_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', next_start_date))
)
-- Final projection
SELECT
    id,
    start_date,
    status,
    premium_filled,
    holder_region,
    next_start_date,
    holder_risk_score_decile,
    end_date_quintile,
    running_premium_filled,
    next_start_date_month_avg_end_date_quintile,
    holder_risk_score_share_of_next_start_date_month,
    next_start_date_month_avg_running_premium_filled,
    first_running_premium_filled
FROM with_windows
ORDER BY holder_risk_score_share_of_next_start_date_month DESC, status, id
LIMIT 10
