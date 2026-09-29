WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE holder_id IS NOT NULL
      AND start_date < DATE '2025-05-28'
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment = 'SME'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        holder_region,
        holder_segment,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY status, holder_segment ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        NTH_VALUE(holder_risk_score, 2) OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        premium - LAG(premium) OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY holder_risk_score DESC, id) AS premium_change,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        MAX(holder_risk_score) OVER (PARTITION BY holder_region, DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_2_prior,
        SUM(holder_risk_score) OVER (PARTITION BY status, holder_segment ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_sum_2_prior,
        SUM(holder_risk_score) OVER (PARTITION BY status, holder_segment ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS holder_risk_score_sum_7,
        LAST_VALUE(holder_risk_score) IGNORE NULLS OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_risk_score,
        LEAD(end_date, 3) OVER (PARTITION BY holder_region, DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id) AS next3_end_date
    FROM enriched_holder
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        holder_region,
        holder_segment,
        second_holder_risk_score,
        premium_change,
        rows_in_status,
        holder_risk_score_max_2_prior,
        last_known_holder_risk_score,
        next3_end_date,
        LEAST(premium, rows_in_status) AS least_premium_rows_in_status
    FROM metrics
),
per_group AS (
    -- Group-level totals
    SELECT
        holder_region,
        MAX(end_date) AS last_end_date,
        PERCENTILE_APPROX(rows_in_status, 0.5) AS median_rows_in_status,
        COUNT(*) AS row_count,
        SUM(second_holder_risk_score) AS total_second_holder_risk_score,
        ROUND(SUM(least_premium_rows_in_status) / NULLIF(SUM(rows_in_status), 0), 3) AS least_premium_rows_in_status_to_rows_in_status_r,
        MIN(least_premium_rows_in_status) AS min_least_premium_rows_in_status
    FROM tagged
    GROUP BY holder_region
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        holder_region,
        median_rows_in_status,
        row_count,
        total_second_holder_risk_score,
        least_premium_rows_in_status_to_rows_in_status_r,
        min_least_premium_rows_in_status,
        ROUND(median_rows_in_status / NULLIF(SUM(median_rows_in_status) OVER (PARTITION BY holder_region), 0), 4) AS median_rows_in_status_share_of_holder_region,
        COUNT(*) OVER w_holder_region AS rows_in_holder_region,
        DENSE_RANK() OVER (ORDER BY row_count) AS row_count_dense_rank,
        MAX(row_count) OVER (PARTITION BY holder_region) AS holder_region_max_row_count
    FROM per_group
    WINDOW w_holder_region AS (PARTITION BY holder_region)
)
SELECT
    holder_region,
    median_rows_in_status,
    least_premium_rows_in_status_to_rows_in_status_r,
    rows_in_holder_region,
    row_count_dense_rank,
    holder_region_max_row_count
FROM ranked
ORDER BY least_premium_rows_in_status_to_rows_in_status_r, holder_region
LIMIT 5
