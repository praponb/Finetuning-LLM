WITH base AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE product <> 'HOME'
      AND YEAR(end_date) = 2026
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment <> 'RETAIL'
    WHERE p.joined_on BETWEEN DATE '2024-02-04' AND DATE '2025-07-03'
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.holder_region,
        s.holder_joined_on,
        p.region AS holder_region_2
    FROM combined_details_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', holder_joined_on) AS holder_joined_on_quarter,
        SUBSTRING(TRIM(holder_region), 1, 3) AS holder_region_prefix
    FROM enriched_holder
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.holder_region,
        s.holder_joined_on,
        s.holder_region_2,
        s.holder_joined_on_quarter,
        s.holder_region_prefix,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at
    FROM labeled s
    JOIN claims c ON c.policy_id = s.id AND c.reported_at < TIMESTAMP '2025-02-12 12:00:00'
)
SELECT
    id,
    premium,
    holder_region_2,
    holder_joined_on_quarter,
    holder_region_prefix,
    claim_id,
    claim_claimed_amount,
    ROUND(claim_claimed_amount * 0.25, 2) AS claim_claimed_amount_adj
FROM line_level
ORDER BY premium
