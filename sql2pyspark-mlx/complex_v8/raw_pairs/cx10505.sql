WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status = 'EXPIRED'
      AND product IN ('HEALTH', 'AUTO')
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        p.segment AS holder_segment
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.holder_segment,
        c.id AS claim_id,
        c.claim_date,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id,
        c.status AS claim_status
    FROM enriched s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    holder_segment,
    claim_date,
    claim_status,
    DATE_TRUNC('WEEK', claim_date) AS claim_date_week
FROM detail_rows
ORDER BY claim_status DESC NULLS FIRST, holder_id DESC
