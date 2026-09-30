WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date >= DATE '2024-03-24'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LEAD(claimed_amount) OVER w_adjuster_id_status_by_report AS next_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY YEAR(reported_at), claim_type) AS reported_at_year_claim_type_max_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY YEAR(reported_at), claim_type)) AS distinct_policy_id_in_reported_at_year_claim_typ
    FROM filtered
    WINDOW w_adjuster_id_status_by_report AS (PARTITION BY adjuster_id, status ORDER BY reported_at, id)
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM windowed s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.policy_id <= 5
        )
),
stats AS (
    -- Summarise per group
    SELECT
        distinct_policy_id_in_reported_at_year_claim_typ,
        adjuster_id,
        MAX(next_claimed_amount) AS max_next_claimed_amount,
        SUM(reported_at_year_claim_type_max_approved_amount) FILTER (WHERE status <> 'PAID') AS reported_at_year_claim_type_max_approved_amount_,
        COUNT(*) FILTER (WHERE claim_type IN ('theft', 'fire', 'flood')) AS n_claim_type_in_theft_fire_flo
    FROM qualified
    GROUP BY distinct_policy_id_in_reported_at_year_claim_typ, adjuster_id
    HAVING AVG(distinct_policy_id_in_reported_at_year_claim_typ) > 2
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount < 41760.8
              AND c.status IN ('APPROVED', 'OPEN', 'IN_REVIEW')
        )
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.distinct_policy_id_in_reported_at_year_claim_typ,
        s.adjuster_id,
        s.max_next_claimed_amount,
        s.reported_at_year_claim_type_max_approved_amount_,
        s.n_claim_type_in_theft_fire_flo,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM screened s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.approved_amount >= 7204.1
)
SELECT
    claim_type,
    n_claim_type_in_theft_fire_flo,
    GROUPING_ID() AS grouping_level,
    MAX(claim_reported_at) AS last_claim_reported_at,
    COUNT(DISTINCT claim_id) AS distinct_claim_ids,
    ROUND(AVG(n_claim_type_in_theft_fire_flo), 2) AS avg_n_claim_type_in_theft_fire_flo
FROM exploded_detail_claims
GROUP BY CUBE(claim_type, n_claim_type_in_theft_fire_flo)
ORDER BY claim_type NULLS LAST, n_claim_type_in_theft_fire_flo NULLS LAST
