WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS(' / ', status, product) AS status_product_label,
        DATE_TRUNC('MONTH', start_date) AS start_date_month
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_status, 0), 4) AS holder_risk_score_share_of_status,
        SUM(premium) OVER (PARTITION BY holder_region, product ORDER BY start_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_sum_2_prior,
        MIN(holder_risk_score) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_holder_risk_score_so_far,
        DENSE_RANK() OVER (PARTITION BY holder_region, status ORDER BY holder_joined_on DESC, id) AS holder_joined_on_dense_rank,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status)) AS distinct_product_in_status,
        COUNT(*) OVER (PARTITION BY holder_region, product ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        NTH_VALUE(holder_risk_score, 2) OVER (PARTITION BY holder_region, status ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        SUM(premium) OVER (PARTITION BY status) AS status_sum_premium,
        ROUND(PERCENT_RANK() OVER w_holder_region_product_by_sta_3, 3) AS start_date_percent_rank
    FROM combined_details_holder
    WINDOW w_status AS (PARTITION BY status), w_holder_region_product_by_sta_3 AS (PARTITION BY holder_region, product ORDER BY start_date DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    start_date_month,
    holder_risk_score,
    holder_risk_score_share_of_status,
    premium_sum_2_prior,
    min_holder_risk_score_so_far,
    distinct_product_in_status,
    premium_share_of_status,
    second_holder_risk_score,
    rows_in_status,
    start_date_percent_rank,
    CASE WHEN holder_risk_score >= 0.6 THEN 'HIGH' WHEN holder_risk_score >= 0.2 THEN 'MEDIUM' ELSE 'LOW' END AS holder_risk_score_tier
FROM with_windows
WHERE holder_region IS NOT NULL
ORDER BY start_date_month, distinct_product_in_status NULLS LAST
