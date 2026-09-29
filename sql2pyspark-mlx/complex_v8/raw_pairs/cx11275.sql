WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id <= 9
      AND YEAR(end_date) = 2026
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(premium) OVER (PARTITION BY product, status), 2) AS product_status_avg_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY start_date ASC, id), 3) AS start_date_cume_dist,
        MAX(premium) OVER (PARTITION BY status ORDER BY start_date ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2
    FROM filtered
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.product_status_avg_premium,
        s.start_date_cume_dist,
        s.premium_max_2,
        p.segment AS holder_segment
    FROM with_windows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        product_status_avg_premium,
        premium_max_2,
        holder_segment,
        INITCAP(TRIM(status)) AS status_clean,
        ROUND(premium_max_2 / NULLIF(premium, 0), 3) AS premium_max_2_per_premium
    FROM with_parent
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(reported_at) AS first_claim_reported_at,
        COUNT(*) AS claim_count,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.max_claim_approved_amount
    FROM labeled s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    end_date,
    premium_max_2,
    status_clean,
    premium_max_2_per_premium,
    claim_count
FROM profiled
WHERE status_clean IS NOT NULL
ORDER BY claim_count, start_date NULLS LAST, id
LIMIT 20
