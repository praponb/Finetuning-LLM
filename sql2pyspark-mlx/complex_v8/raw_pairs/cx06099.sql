WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE premium >= 2096.6
      AND end_date < DATE '2026-11-25'
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.*,
        p.segment AS holder_segment,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        premium,
        end_date,
        product_clean,
        holder_joined_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY premium DESC NULLS FIRST), 3) AS premium_percent_rank,
        LEAD(end_date) OVER (ORDER BY premium, id) AS next_end_date,
        SUM(premium) OVER (PARTITION BY holder_segment, product) AS holder_segment_product_sum_premium,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, holder_id ORDER BY end_date DESC NULLS FIRST, id), 3) AS end_date_percent_rank,
        SUM(premium) OVER (PARTITION BY status ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        COUNT(*) OVER (PARTITION BY holder_segment, product) AS rows_in_holder_segment_product
    FROM with_parent
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM windowed s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
SELECT id, field, metric_value
FROM (
    SELECT id, claim_count, rows_in_holder_segment_product
    FROM with_stats
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (claim_count, rows_in_holder_segment_product)
) unp
ORDER BY id, field
