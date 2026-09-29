WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM policies
    WHERE product = 'LIFE'
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.days_since_start_date,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM valid_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_holder_id_holder_segment_by AS days_since_start_date_dense_rank,
        COUNT(*) OVER (PARTITION BY status, holder_region) AS rows_in_status_holder_region,
        LAST_VALUE(days_since_start_date) OVER (PARTITION BY status ORDER BY days_since_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_start_date,
        DENSE_RANK() OVER w_status_holder_region_by_days AS days_since_start_date_dense_rank_2,
        LAST_VALUE(days_since_start_date) OVER (PARTITION BY status, holder_region ORDER BY days_since_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_start_date_2,
        MIN(holder_risk_score) OVER w_status_holder_region AS status_holder_region_min_holder_risk_score,
        LAG(holder_risk_score, 1, 0) OVER w_holder_id_holder_segment_by AS prev_holder_risk_score,
        LEAD(days_since_start_date) OVER (PARTITION BY holder_id, holder_segment ORDER BY days_since_start_date, id) AS next_days_since_start_date,
        ROUND(PERCENT_RANK() OVER w_status_by_days_since_start_d_2, 3) AS days_since_start_date_percent_rank
    FROM with_details
    WINDOW w_holder_id_holder_segment_by AS (PARTITION BY holder_id, holder_segment ORDER BY days_since_start_date, id), w_status_holder_region_by_days AS (PARTITION BY status, holder_region ORDER BY days_since_start_date), w_status_holder_region AS (PARTITION BY status, holder_region), w_status_by_days_since_start_d_2 AS (PARTITION BY status ORDER BY days_since_start_date DESC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        holder_id,
        status,
        days_since_start_date,
        holder_segment,
        holder_risk_score,
        holder_region,
        days_since_start_date_dense_rank,
        rows_in_status_holder_region,
        last_days_since_start_date,
        last_days_since_start_date_2,
        status_holder_region_min_holder_risk_score,
        prev_holder_risk_score,
        next_days_since_start_date,
        days_since_start_date_percent_rank
    FROM with_windows
    WHERE days_since_start_date_dense_rank_2 <= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        status,
        days_since_start_date,
        holder_risk_score,
        holder_region,
        days_since_start_date_dense_rank,
        rows_in_status_holder_region,
        last_days_since_start_date_2,
        status_holder_region_min_holder_risk_score,
        prev_holder_risk_score,
        days_since_start_date_percent_rank,
        ROUND(days_since_start_date / NULLIF(SUM(days_since_start_date) OVER (PARTITION BY days_since_start_date_dense_rank, holder_segment), 0), 4) AS days_since_start_date_share_of_days_since_start_,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY days_since_start_date_dense_rank, holder_segment)) AS distinct_status_in_days_since_start_date_dense_r,
        ROUND(AVG(prev_holder_risk_score) OVER (PARTITION BY holder_segment, holder_id ORDER BY next_days_since_start_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prev_holder_risk_score_avg_7,
        COUNT(*) OVER w_days_since_start_date_dense AS rows_in_days_since_start_date_dense_rank_holder_,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_days_since_start_date_dense, 0), 4) AS holder_risk_score_share_of_days_since_start_date,
        DENSE_RANK() OVER (ORDER BY next_days_since_start_date DESC) AS next_days_since_start_date_dense_rank
    FROM top_rows
    WINDOW w_days_since_start_date_dense AS (PARTITION BY days_since_start_date_dense_rank, holder_segment)
)
SELECT
    id,
    holder_id,
    holder_risk_score,
    holder_region,
    days_since_start_date_dense_rank,
    rows_in_status_holder_region,
    status_holder_region_min_holder_risk_score,
    prev_holder_risk_score,
    days_since_start_date_share_of_days_since_start_,
    prev_holder_risk_score_avg_7,
    holder_risk_score_share_of_days_since_start_date,
    next_days_since_start_date_dense_rank
FROM windowed
ORDER BY prev_holder_risk_score NULLS LAST, id DESC
LIMIT 5
