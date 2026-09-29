WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
    WHERE product = 'HOME'
      AND status = 'EXPIRED'
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE LOWER(p.holder_name) LIKE '%on%'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER (PARTITION BY product, holder_id), 0), 4) AS premium_adj_share_of_product_holder_id,
        LAG(premium_adj) OVER (ORDER BY premium_adj, id) AS prev_premium_adj,
        ROUND(CUME_DIST() OVER (ORDER BY premium_adj, id), 3) AS premium_adj_cume_dist,
        SIZE(COLLECT_SET(holder_segment) OVER (PARTITION BY product, holder_id)) AS distinct_holder_segment_in_product_holder_id,
        LAST_VALUE(premium_adj) OVER (ORDER BY premium_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_adj,
        NTH_VALUE(holder_risk_score, 2) OVER (ORDER BY premium_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY product, holder_id), 2) AS product_holder_id_avg_holder_risk_score,
        ROUND(AVG(holder_risk_score) OVER (ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS holder_risk_score_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC NULLS FIRST, id), 3) AS premium_percent_rank,
        COUNT(*) OVER (PARTITION BY product, holder_id) AS rows_in_product_holder_id,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_holder_id, 0), 4) AS premium_share_of_product_holder_id
    FROM combined_details
    WINDOW w_product_holder_id AS (PARTITION BY product, holder_id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(holder_risk_score / NULLIF(premium_adj_cume_dist, 0), 3) AS holder_risk_score_per_premium_adj_cume_dist,
        COALESCE(premium_percent_rank, 0.0) AS premium_percent_rank_filled,
        LENGTH(TRIM(status)) AS status_len
    FROM with_windows
    WHERE last_premium_adj BETWEEN 1660.0 AND 6040.0
),
summary AS (
    -- Group-level totals
    SELECT
        holder_segment,
        distinct_holder_segment_in_product_holder_id,
        holder_id,
        ROUND(AVG(premium_adj_share_of_product_holder_id), 2) AS avg_premium_adj_share_of_product_holder_id,
        SUM(rows_in_product_holder_id) FILTER (WHERE product IS NOT NULL) AS rows_in_product_holder_id_product_is_not_null
    FROM tagged
    GROUP BY holder_segment, distinct_holder_segment_in_product_holder_id, holder_id
    HAVING SUM(product_holder_id_avg_holder_risk_score) > 0.2
)
-- Final projection
SELECT
    holder_segment,
    distinct_holder_segment_in_product_holder_id,
    holder_id,
    avg_premium_adj_share_of_product_holder_id
FROM summary
WHERE holder_segment IS NOT NULL
ORDER BY avg_premium_adj_share_of_product_holder_id
