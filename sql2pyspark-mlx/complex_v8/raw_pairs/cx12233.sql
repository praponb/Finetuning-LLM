WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE premium <= 3532.7
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id, product)) AS distinct_status_in_holder_id_product,
        RANK() OVER (PARTITION BY product, status ORDER BY premium ASC, id) AS premium_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, product), 0), 4) AS premium_share_of_holder_id_product,
        MAX(holder_risk_score) OVER (PARTITION BY status ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS holder_risk_score_max_7,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY holder_id, product), 2) AS holder_risk_score_vs_holder_id_product_avg,
        SIZE(COLLECT_SET(holder_region) OVER (PARTITION BY holder_id, product)) AS distinct_holder_region_in_holder_id_product,
        COUNT(*) OVER w_holder_id_product AS rows_in_holder_id_product,
        RANK() OVER (PARTITION BY product ORDER BY premium, id) AS premium_rank_2,
        NTILE(3) OVER w_product_status_by_premium AS premium_tercile,
        MIN(premium) OVER (PARTITION BY holder_id, product) AS holder_id_product_min_premium,
        SUM(premium) OVER (PARTITION BY status ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_id, product), 0), 4) AS holder_risk_score_share_of_holder_id_product,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, product), 2) AS premium_vs_holder_id_product_avg,
        MAX(premium) OVER (PARTITION BY holder_id, product) AS holder_id_product_max_premium,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product ORDER BY premium), 3) AS premium_percent_rank
    FROM enriched_holder
    WINDOW w_holder_id_product AS (PARTITION BY holder_id, product), w_product_status_by_premium AS (PARTITION BY product, status ORDER BY premium ASC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        status,
        holder_risk_score,
        holder_region,
        premium_rank,
        holder_risk_score_max_7,
        holder_risk_score_vs_holder_id_product_avg,
        rows_in_holder_id_product,
        premium_tercile,
        holder_id_product_min_premium,
        running_premium,
        holder_risk_score_share_of_holder_id_product,
        premium_percent_rank,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        LENGTH(TRIM(holder_region)) AS holder_region_len,
        LOWER(REGEXP_REPLACE(TRIM(holder_region), ' ', '_')) AS holder_region_slug
    FROM sequenced
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(holder_risk_score_max_7) OVER (PARTITION BY holder_region_len, status) AS holder_region_len_status_max_holder_risk_score_m,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, holder_region)) AS distinct_holder_id_in_status_holder_region,
        MIN(holder_risk_score) OVER (PARTITION BY holder_region_len, status) AS holder_region_len_status_min_holder_risk_score,
        LEAD(premium_tercile, 1, 0) OVER w_status_premium_tercile_by_ho AS next_premium_tercile,
        ROUND(AVG(rows_in_holder_id_product) OVER (PARTITION BY holder_id, holder_region ORDER BY holder_risk_score_max_7 ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rows_in_holder_id_product_avg_7,
        MAX(premium_percent_rank) OVER (ORDER BY premium_percent_rank DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_percent_rank_max_3
    FROM annotated
    WINDOW w_status_premium_tercile_by_ho AS (PARTITION BY status, premium_tercile ORDER BY holder_risk_score_max_7 DESC, id)
)
SELECT
    id,
    holder_id,
    product,
    status,
    holder_risk_score,
    holder_region,
    premium_rank,
    holder_risk_score_max_7,
    rows_in_holder_id_product,
    premium_tercile,
    holder_id_product_min_premium,
    running_premium,
    holder_risk_score_share_of_holder_id_product,
    premium_percent_rank,
    product_slug,
    holder_region_len,
    holder_region_len_status_max_holder_risk_score_m,
    distinct_holder_id_in_status_holder_region,
    holder_region_len_status_min_holder_risk_score,
    next_premium_tercile,
    rows_in_holder_id_product_avg_7
FROM with_windows
WHERE holder_risk_score_share_of_holder_id_product BETWEEN 0.1 AND 0.8
ORDER BY premium_tercile DESC
