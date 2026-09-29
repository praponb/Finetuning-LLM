WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id <= 15
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        p.holder_name
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on))) AS distinct_status_in_holder_joined_on_month,
        ROUND(AVG(holder_risk_score) OVER (ORDER BY end_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS holder_risk_score_avg_2_prior,
        DENSE_RANK() OVER w_all_by_end_date AS end_date_dense_rank,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on)), 0), 4) AS holder_risk_score_share_of_holder_joined_on_mont,
        MIN(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on)) AS holder_joined_on_month_min_holder_risk_score,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER (PARTITION BY YEAR(end_date) ORDER BY end_date, id)) AS days_since_prev_holder_joined_on,
        ROUND(AVG(holder_risk_score) OVER (ORDER BY end_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_4,
        COUNT(*) OVER w_holder_joined_on_month AS rows_in_holder_joined_on_month,
        LAST_VALUE(start_date) OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROUND(CUME_DIST() OVER w_end_date_year_by_end_date_ti, 3) AS end_date_cume_dist,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY YEAR(end_date) ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_7,
        SUM(holder_risk_score) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        SUM(holder_risk_score) OVER (PARTITION BY YEAR(end_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score_2
    FROM joined_holder
    WINDOW w_all_by_end_date AS (ORDER BY end_date DESC, id), w_holder_joined_on_month AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on)), w_end_date_year_by_end_date_ti AS (PARTITION BY YEAR(end_date) ORDER BY end_date)
)
SELECT
    id,
    holder_id,
    end_date,
    status,
    holder_risk_score,
    holder_name,
    end_date_dense_rank,
    holder_risk_score_share_of_holder_joined_on_mont,
    rows_in_holder_joined_on_month,
    last_start_date,
    holder_risk_score_avg_7,
    running_holder_risk_score,
    running_holder_risk_score_2,
    LEAST(holder_risk_score, holder_risk_score_avg_4) AS least_holder_risk_score_holder_risk_score_avg_4,
    CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group
FROM with_trends
ORDER BY least_holder_risk_score_holder_risk_score_avg_4
