WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('LAPSED', 'ACTIVE') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE end_date < DATE '2026-11-25'
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(product_or_unknown, '#', CAST(id AS STRING)) AS product_or_unknown_ref
    FROM valid
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status_group,
        s.product_or_unknown,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at
    FROM annotated s
    LEFT JOIN claims c ON c.policy_id = s.id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_children_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.claim_adjuster_id
              AND HOUR(c.reported_at) BETWEEN 6 AND 12
        )
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status_group,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_claimed_amount,
        s.claim_reported_at,
        c.id AS claim_id_2,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id_2,
        c.claim_date
    FROM qualified s
    LEFT JOIN claims c ON c.policy_id = s.id
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status_group,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_claimed_amount,
        s.claim_reported_at,
        s.claim_id_2,
        s.claim_adjuster_id_2,
        s.claim_date,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM line_level s
    JOIN adjusters a ON a.id = s.claim_adjuster_id_2
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status_group,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_id_2,
        s.claim_adjuster_id_2,
        s.adjuster_name,
        s.adjuster_level,
        c.id AS claim_id_3,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at_2
    FROM joined_adjuster s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    start_date,
    status_group,
    claim_id,
    claim_adjuster_id,
    claim_id_2,
    claim_adjuster_id_2,
    adjuster_name,
    adjuster_level,
    claim_id_3,
    claim_status,
    claim_approved_amount,
    claim_reported_at_2,
    TRUNC(end_date, 'MM') AS end_date_month_start,
    ABS(adjuster_level - 3) AS adjuster_level_distance
FROM exploded_detail
ORDER BY start_date DESC
