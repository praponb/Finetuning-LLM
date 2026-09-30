WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month
    FROM claims
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM cleaned s
)
SELECT
    adjuster_id,
    claim_count,
    COALESCE(claim_type_or_unknown, 'ALL') AS claim_type_or_unknown,
    ROUND(AVG(claim_count), 2) AS avg_claim_count,
    PERCENTILE_APPROX(claim_count, 0.5) AS median_claim_count
FROM summarized_rows
GROUP BY CUBE(adjuster_id, claim_count, claim_type_or_unknown)
