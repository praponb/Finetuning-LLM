WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE adjuster_id <= 5
      AND status IN ('IN_REVIEW', 'OPEN', 'APPROVED')
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_date_label,
        s.approved_amount_filled,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.end_date AS policy_end_date
    FROM valid s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        SUM(approved_amount) AS total_approved_amount
    FROM enriched_adjuster
    GROUP BY adjuster_id
)
SELECT
    adjuster_id,
    total_approved_amount,
    ABS(total_approved_amount - 195000.0) AS total_approved_amount_distance,
    ARRAY_JOIN(ARRAY_SORT(status_list), ',') AS status_list_text
FROM rollup_base
