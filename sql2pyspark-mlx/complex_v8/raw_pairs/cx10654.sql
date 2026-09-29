WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LAST_DAY(start_date) AS start_date_month_end,
        DATE_ADD(end_date, 365) AS end_date_plus_365d
    FROM policies
    WHERE status <> 'CANCELLED'
      AND product <> 'HEALTH'
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.start_date_month_end,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.holder_joined_on,
        s.holder_region,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount
    FROM enriched_holder s
    JOIN claims c ON c.policy_id = s.id
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.claim_adjuster_id
              AND a.team = 'Motor'
              AND a.level >= 1
        )
),
with_parent AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.holder_joined_on,
        s.holder_region,
        s.claim_id,
        s.claim_status,
        s.claim_adjuster_id,
        s.claim_approved_amount,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM qualified s
    LEFT JOIN adjusters a ON a.id = s.claim_adjuster_id
),
filtered AS (
    -- Keep rows whose claim matches
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.id = s.claim_id
              AND c.claim_type <> 'medical'
        )
)
-- Final projection
SELECT
    id,
    premium,
    holder_joined_on,
    holder_region,
    claim_id,
    claim_adjuster_id,
    claim_approved_amount,
    adjuster_name,
    adjuster_team
FROM filtered
ORDER BY id NULLS LAST, adjuster_name
