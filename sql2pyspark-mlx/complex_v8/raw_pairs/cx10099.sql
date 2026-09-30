WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
    WHERE holder_id >= 7
      AND YEAR(start_date) = 2025
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at,
        (SELECT COUNT(CASE WHEN c.status IS NOT NULL THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_status_count,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date
    FROM prepared s
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.product_prefix,
        s.last_claim_reported_at,
        s.claim_status_count,
        p.holder_name
    FROM with_counts s
    JOIN policyholders p ON p.id = s.holder_id
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.claim_status_count,
        s.holder_name,
        c.id AS claim_id,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM enriched_holder s
    JOIN claims c ON c.policy_id = s.id
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM detail_rows_claims s
    JOIN adjusters a ON a.id = s.claim_adjuster_id
    WHERE a.team IN ('Property', 'Health', 'Motor')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        claim_status_count,
        claim_id,
        claim_type,
        claim_adjuster_id,
        adjuster_team,
        adjuster_name,
        adjuster_level,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        COALESCE(adjuster_level, 0) AS adjuster_level_filled
    FROM with_details_adjuster
    WHERE product <> 'HOME'
)
-- Final projection
SELECT
    id,
    claim_status_count,
    claim_id,
    claim_type,
    adjuster_team,
    adjuster_name,
    adjuster_level,
    claim_type_or_unknown,
    adjuster_level_filled,
    CONCAT(adjuster_name, '#', CAST(claim_status_count AS STRING)) AS adjuster_name_ref,
    INITCAP(TRIM(adjuster_name)) AS adjuster_name_clean
FROM enriched
ORDER BY claim_id
