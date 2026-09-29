WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        CASE WHEN premium >= 4417.0 THEN 'large' WHEN premium >= 2013.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE holder_id IS NOT NULL
      AND end_date < DATE '2026-11-25'
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_tier,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at,
        c.claim_type,
        c.claimed_amount AS claim_claimed_amount
    FROM cleaned s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.claim_type IS NOT NULL
)
SELECT
    id,
    start_date,
    end_date,
    status,
    premium_tier,
    claim_id,
    claim_type,
    claim_claimed_amount
FROM line_level
