WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('TRAVEL', 'LIFE') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group,
        LAST_DAY(start_date) AS start_date_month_end
    FROM policies
    WHERE premium > 2289.0
      AND product = 'HEALTH'
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.product_group,
        s.start_date_month_end,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(holder_risk_score) OVER w_status AS status_min_holder_risk_score,
        holder_risk_score - LAG(holder_risk_score) OVER w_product_by_start_date_month AS holder_risk_score_change,
        SUM(holder_risk_score) OVER (PARTITION BY status) AS status_sum_holder_risk_score,
        LAST_VALUE(holder_risk_score) IGNORE NULLS OVER (ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_risk_score,
        ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY start_date_month_end DESC, id) AS rn,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY status), 2) AS status_avg_holder_risk_score,
        SIZE(COLLECT_SET(product_group) OVER w_status) AS distinct_product_group_in_status,
        LAST_VALUE(holder_risk_score) IGNORE NULLS OVER (PARTITION BY product_group ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_risk_score_2,
        NTH_VALUE(holder_risk_score, 2) OVER (ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        LAG(holder_risk_score) OVER w_all_by_start_date_month_end_3 AS prev_holder_risk_score,
        COUNT(*) OVER (ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        holder_risk_score - LAG(holder_risk_score) OVER w_product_group_by_start_date AS holder_risk_score_change_2,
        COUNT(*) OVER (PARTITION BY product_group) AS rows_in_product_group,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY product_group), 0), 4) AS holder_risk_score_share_of_product_group
    FROM with_parent_holder
    WINDOW w_status AS (PARTITION BY status), w_product_by_start_date_month AS (PARTITION BY product ORDER BY start_date_month_end, id), w_all_by_start_date_month_end_3 AS (ORDER BY start_date_month_end DESC, id), w_product_group_by_start_date AS (PARTITION BY product_group ORDER BY start_date_month_end DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(holder_risk_score - 0.6) AS holder_risk_score_distance,
        ABS(rows_in_status - 13) AS rows_in_status_distance,
        UPPER(TRIM(product_group)) AS product_group_clean
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(holder_risk_score) OVER (ORDER BY status_sum_holder_risk_score DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS holder_risk_score_avg_1_prior,
        LAG(start_date_month_end) OVER (PARTITION BY holder_id ORDER BY status_sum_holder_risk_score DESC, id) AS prev_start_date_month_end,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY status_sum_holder_risk_score DESC, id), 3) AS status_sum_holder_risk_score_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY status_sum_holder_risk_score DESC NULLS FIRST), 3) AS status_sum_holder_risk_score_percent_rank
    FROM annotated
)
SELECT
    id,
    product,
    status,
    start_date_month_end,
    holder_risk_score,
    status_min_holder_risk_score,
    holder_risk_score_change,
    status_sum_holder_risk_score,
    last_known_holder_risk_score,
    rn,
    status_avg_holder_risk_score,
    distinct_product_group_in_status,
    last_known_holder_risk_score_2,
    second_holder_risk_score,
    running_count,
    holder_risk_score_change_2,
    rows_in_product_group,
    product_group_clean,
    holder_risk_score_avg_1_prior,
    status_sum_holder_risk_score_percent_rank,
    DATE_TRUNC('YEAR', start_date_month_end) AS start_date_month_end_year,
    LOWER(TRIM(product)) AS product_clean
FROM metrics
WHERE product_group_clean IS NOT NULL
ORDER BY distinct_product_group_in_status DESC NULLS FIRST, id
LIMIT 5
