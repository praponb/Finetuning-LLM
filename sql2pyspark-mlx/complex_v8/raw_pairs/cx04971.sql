WITH prepared AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE product IS NOT NULL
      AND status NOT IN ('LAPSED', 'ACTIVE')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        p.risk_score AS holder_risk_score
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        premium + holder_risk_score AS premium_plus_holder_risk_score,
        ROUND(holder_risk_score * 0.25, 2) AS holder_risk_score_adj
    FROM with_parent
    WHERE holder_id <= 11
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(DISTINCT status) AS distinct_status,
        PERCENTILE_APPROX(premium_plus_holder_risk_score, 0.5) AS median_premium_plus_holder_risk_score
    FROM tagged
    GROUP BY product
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_status, product), 3) AS distinct_status_percent_rank,
        LEAD(distinct_status) OVER (ORDER BY distinct_holder_ids DESC NULLS FIRST, product) AS next_distinct_status,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        MAX(distinct_holder_ids) OVER (ORDER BY distinct_status, product ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_distinct_holder_ids_so_far,
        LAST_VALUE(distinct_holder_ids) OVER (ORDER BY distinct_status, product ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_holder_ids,
        NTILE(5) OVER (ORDER BY distinct_status, product) AS distinct_status_quintile
    FROM summary
),
derived AS (
    -- Derive helper columns
    SELECT
        product,
        distinct_holder_ids,
        distinct_status,
        median_premium_plus_holder_risk_score,
        distinct_status_percent_rank,
        next_distinct_status,
        distinct_status_quintile,
        ROUND(last_distinct_holder_ids / NULLIF(rows_in_product, 0), 3) AS last_distinct_holder_ids_per_rows_in_product,
        COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled
    FROM metrics
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        product,
        distinct_holder_ids,
        distinct_status,
        median_premium_plus_holder_risk_score,
        next_distinct_status,
        distinct_status_quintile,
        distinct_holder_ids_filled,
        DENSE_RANK() OVER w_distinct_status_quintile_dis AS distinct_status_dense_rank,
        NTILE(10) OVER w_distinct_status_by_next_dist AS next_distinct_status_decile,
        ROUND(distinct_holder_ids_filled - AVG(distinct_holder_ids_filled) OVER (PARTITION BY distinct_holder_ids, distinct_status_quintile), 2) AS distinct_holder_ids_filled_vs_distinct_holder_id
    FROM derived
    WINDOW w_distinct_status_quintile_dis AS (PARTITION BY distinct_status_quintile, distinct_holder_ids ORDER BY distinct_status, product), w_distinct_status_by_next_dist AS (PARTITION BY distinct_status ORDER BY next_distinct_status DESC, product)
)
-- Final projection
SELECT
    product,
    distinct_holder_ids,
    distinct_status,
    median_premium_plus_holder_risk_score,
    next_distinct_status,
    distinct_status_quintile,
    distinct_holder_ids_filled,
    distinct_status_dense_rank,
    next_distinct_status_decile,
    distinct_holder_ids_filled_vs_distinct_holder_id
FROM metrics_2
ORDER BY distinct_status DESC
