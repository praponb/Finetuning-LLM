WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref,
        LAST_DAY(start_date) AS start_date_month_end
    FROM policies
    WHERE premium > 1132.1
      AND status <> 'LAPSED'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.product_ref,
        s.start_date_month_end,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product), 2) AS premium_vs_product_avg,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY product), 2) AS holder_risk_score_vs_product_avg,
        DENSE_RANK() OVER (ORDER BY premium) AS premium_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY end_date, id), 3) AS end_date_cume_dist,
        SUM(premium) OVER (ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_sum_6_prior,
        SUM(premium) OVER (PARTITION BY holder_segment ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(AVG(premium) OVER (ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        MIN(premium) OVER w_product AS product_min_premium,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_segment ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date
    FROM combined_details
    WINDOW w_product AS (PARTITION BY product)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    product_ref,
    holder_risk_score,
    premium_vs_product_avg,
    holder_risk_score_vs_product_avg,
    premium_dense_rank,
    end_date_cume_dist,
    running_premium,
    premium_avg_1_prior,
    product_min_premium,
    COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown
FROM with_trends
WHERE holder_id IS NOT NULL
ORDER BY running_premium DESC NULLS FIRST, premium_vs_product_avg DESC
