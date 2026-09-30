WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS('-', claim_type, status) AS claim_type_status_label,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-03-08 10:00:00'
      AND status IN ('OPEN', 'PAID', 'IN_REVIEW')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_claimed_amount AS seq_no,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id), 0), 4) AS claimed_amount_share_of_claim_type_adjuster_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        SUM(claimed_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        MIN(approved_amount) OVER w_claim_type_adjuster_id AS claim_type_adjuster_id_min_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY claim_type, adjuster_id), 2) AS claim_type_adjuster_id_avg_approved_amount
    FROM valid_claims
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id), w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref,
        TO_DATE(reported_at) AS reported_at_date,
        running_claimed_amount - claimed_amount AS running_claimed_amount_minus_claimed_amount
    FROM sequenced
),
scored_2 AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek,
        GREATEST(claimed_amount, running_claimed_amount) AS greatest_claimed_amount_running_claimed_amount
    FROM scored
    WHERE seq_no < 8
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM scored_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status IS NOT NULL
        )
      AND s.claim_type IS NOT NULL
),
scored_3 AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        TO_DATE(reported_at) AS reported_at_date_2,
        CONCAT_WS('|', status_ref, claim_type_status_label) AS status_ref_claim_type_status_label_label
    FROM qualified
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored_3
    WHERE approved_amount > (SELECT AVG(approved_amount) FROM scored_3)
)
-- Final projection
SELECT DISTINCT id
FROM screened
WHERE running_claimed_amount > 175065.0
