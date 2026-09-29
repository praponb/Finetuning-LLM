WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE (start_date >= DATE '2025-01-24' OR end_date < DATE '2026-11-25')
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status_prefix,
        c.id AS claim_id,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM cleaned_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date >= DATE '2025-01-24'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        start_date,
        status_prefix,
        claim_id,
        claim_date,
        claim_claimed_amount,
        claim_status,
        claim_reported_at,
        CASE WHEN claim_status IN ('REJECTED', 'PAID') THEN 'group_a' WHEN claim_status = 'OPEN' THEN 'group_b' ELSE 'other' END AS claim_status_group,
        YEAR(claim_date) AS claim_date_year
    FROM qualified
    WHERE claim_claimed_amount > 12010.5
),
without_matches AS (
    -- Keep rows whose claim matches
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.id = s.claim_id
              AND c.claim_date < DATE '2024-12-30'
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        start_date,
        status_prefix,
        claim_id,
        claim_date,
        claim_status,
        claim_reported_at,
        claim_status_group,
        claim_date_year,
        COALESCE(claim_status, 'UNKNOWN') AS claim_status_or_unknown,
        CONCAT(claim_status_group, '#', CAST(claim_date_year AS STRING)) AS claim_status_group_ref
    FROM without_matches
    WHERE claim_date_year BETWEEN 2024 AND 2026
),
with_details AS (
    -- Join claims
    SELECT s.*, c.claimed_amount AS claim_claimed_amount
    FROM scored s
    LEFT JOIN claims c ON c.id = s.claim_id AND c.status NOT IN ('APPROVED', 'PAID')
)
-- Final projection
SELECT
    id,
    start_date,
    status_prefix,
    claim_id,
    claim_status,
    claim_reported_at,
    claim_status_group,
    claim_date_year,
    claim_status_group_ref,
    claim_claimed_amount,
    CONCAT_WS(' / ', claim_status_group_ref, status_prefix) AS claim_status_group_ref_status_prefix_label
FROM with_details
