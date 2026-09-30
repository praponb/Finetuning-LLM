WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        YEAR(end_date) AS end_date_year
    FROM policies
    WHERE end_date BETWEEN DATE '2025-06-08' AND DATE '2026-11-25'
      AND holder_id >= 9
),
enriched AS (
    -- Join policyholders
    SELECT
        s.*,
        p.holder_name,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM base s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.region = 'South'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(end_date_year) OVER (PARTITION BY product, holder_region) AS product_holder_region_sum_end_date_year,
        COUNT(*) OVER (PARTITION BY product, holder_region) AS rows_in_product_holder_region
    FROM enriched
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'CANCELLED'
              AND p.start_date BETWEEN DATE '2024-01-25' AND DATE '2025-10-13'
        )
      AND s.end_date_year <= 2026
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        holder_name,
        holder_region,
        holder_risk_score,
        product_holder_region_sum_end_date_year,
        rows_in_product_holder_region,
        SUM(premium) OVER (PARTITION BY product, holder_id ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(CUME_DIST() OVER (ORDER BY end_date_year DESC), 3) AS end_date_year_cume_dist
    FROM without_matches
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region NOT IN ('South', 'East')
        )
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.holder_name,
        s.holder_region,
        s.holder_risk_score,
        s.product_holder_region_sum_end_date_year,
        s.rows_in_product_holder_region,
        s.running_premium,
        s.end_date_year_cume_dist,
        c.id AS claim_id,
        c.claim_date,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM screened s
    JOIN claims c ON c.policy_id = s.id
)
SELECT id, claim_id, metric_name, value
FROM (
    SELECT id, claim_id, rows_in_product_holder_region, product_holder_region_sum_end_date_year
    FROM with_children_claims
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (rows_in_product_holder_region AS rows_in_product_holder_region_v, product_holder_region_sum_end_date_year AS product_holder_region_sum_end_date_year_v)
) unp
