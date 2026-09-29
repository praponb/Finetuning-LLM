WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(product)) AS product_clean,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
    WHERE holder_id IS NOT NULL
      AND end_date < DATE '2026-06-18'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(start_date_month_start) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), MONTH(start_date) ORDER BY start_date_month_start DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date_month_start,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), MONTH(start_date) ORDER BY start_date_month_start DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), MONTH(start_date) ORDER BY start_date_month_start DESC NULLS FIRST), 3) AS start_date_month_start_cume_dist
    FROM filtered_policys
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM windowed s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY product, YEAR(end_date) ORDER BY start_date_month_start_cume_dist NULLS LAST) AS start_date_month_start_cume_dist_rank,
        FIRST_VALUE(claim_count) OVER (PARTITION BY product, YEAR(end_date) ORDER BY start_date_month_start_cume_dist NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_count,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY claim_count, DATE_TRUNC('MONTH', end_date)), 0), 4) AS claim_count_share_of_claim_count_end_date_month,
        DENSE_RANK() OVER (PARTITION BY product, YEAR(end_date) ORDER BY start_date_month_start_cume_dist NULLS LAST) AS start_date_month_start_cume_dist_dense_rank
    FROM summarized_rows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT start_date, status, start_date_month_start_cume_dist
    FROM with_trends
) src
PIVOT (
    SUM(start_date_month_start_cume_dist)
    FOR status IN ('EXPIRED', 'CANCELLED', 'LAPSED', 'ACTIVE')
)
