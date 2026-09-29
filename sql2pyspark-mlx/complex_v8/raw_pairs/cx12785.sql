WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        ABS(claimed_amount - 30115.9) AS claimed_amount_distance,
        CONCAT_WS('|', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE policy_id >= 17
      AND claimed_amount <= 43724.9
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(reported_at) AS reported_at_year,
        TO_DATE(reported_at) AS reported_at_date
    FROM valid
    WHERE claim_type_status_label IS NOT NULL
),
with_activity AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team = 'Property'
              AND LENGTH(TRIM(a.adjuster_name)) >= 8
        )
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(reported_at) AS last_claim_reported_at,
        COUNT(*) AS claim_count,
        MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.first_claim_reported_at
    FROM with_activity s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.claim_type_prefix,
        s.claim_type_status_label,
        s.reported_at_date,
        s.last_claim_reported_at,
        s.claim_count,
        s.first_claim_reported_at,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM summarized_rows s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    reported_at_date,
    first_claim_reported_at,
    adjuster_team,
    adjuster_name,
    CONCAT_WS('|', adjuster_name, adjuster_team) AS adjuster_name_adjuster_team_label
FROM joined_adjuster
WHERE policy_id IS NOT NULL
ORDER BY policy_id, claim_date NULLS LAST
