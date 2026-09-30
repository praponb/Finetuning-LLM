WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status IS NOT NULL
),
with_parent AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status_len,
        p.region AS holder_region,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM prepared_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(start_date) OVER (PARTITION BY holder_id, product ORDER BY end_date DESC, id) AS next_start_date,
        MAX(holder_risk_score) OVER (PARTITION BY holder_region) AS holder_region_max_holder_risk_score,
        LEAD(holder_risk_score, 1, 0) OVER (ORDER BY holder_risk_score ASC, id) AS next_holder_risk_score,
        DATEDIFF(start_date, LAG(start_date) OVER w_all_by_holder_risk_score) AS days_since_prev_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_holder_id_product_by_end_dat) AS days_since_prev_start_date_2,
        ROW_NUMBER() OVER (PARTITION BY holder_region ORDER BY status_len DESC, id) AS status_len_seq,
        SIZE(COLLECT_SET(product) OVER w_holder_region) AS distinct_product_in_holder_region,
        MIN(premium) OVER (PARTITION BY holder_region, status_len) AS holder_region_status_len_min_premium,
        SUM(status_len) OVER (PARTITION BY holder_region ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_len,
        NTILE(3) OVER w_holder_id_product_by_end_dat AS end_date_tercile
    FROM with_parent
    WINDOW w_all_by_holder_risk_score AS (ORDER BY holder_risk_score ASC, id), w_holder_id_product_by_end_dat AS (PARTITION BY holder_id, product ORDER BY end_date DESC, id), w_holder_region AS (PARTITION BY holder_region)
)
SELECT
    id,
    product,
    premium,
    start_date,
    status_len,
    holder_region,
    holder_name,
    holder_risk_score,
    next_start_date,
    holder_region_max_holder_risk_score,
    days_since_prev_start_date,
    days_since_prev_start_date_2,
    status_len_seq,
    distinct_product_in_holder_region,
    holder_region_status_len_min_premium,
    end_date_tercile,
    COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown,
    COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown_2
FROM with_trends
ORDER BY holder_region_max_holder_risk_score, days_since_prev_start_date DESC
