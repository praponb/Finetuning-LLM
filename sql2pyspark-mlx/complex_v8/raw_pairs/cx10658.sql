WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        COALESCE(premium, 0.0) AS premium_filled,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        SUM(premium) AS total_policy_premium,
        MAX(start_date) AS last_policy_start_date,
        COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.total_policy_premium,
        p.last_policy_start_date,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM filtered s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.premium_filled,
        s.start_date_label,
        s.total_policy_premium,
        s.last_policy_start_date,
        s.policy_count,
        p.segment AS holder_segment
    FROM with_stats s
    JOIN policyholders p ON p.id = s.holder_id
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium_filled,
        s.start_date_label,
        s.total_policy_premium,
        s.policy_count,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date
    FROM with_details_holder s
    JOIN claims c ON c.policy_id = s.id AND c.claim_date BETWEEN DATE '2024-06-27' AND DATE '2025-05-30'
)
SELECT
    claim_id,
    holder_id,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(total_policy_premium, 0.5) AS median_total_policy_premium,
    MIN(claim_date) AS first_claim_date
FROM with_children
GROUP BY CUBE(claim_id, holder_id)
ORDER BY claim_id NULLS LAST, holder_id NULLS LAST
