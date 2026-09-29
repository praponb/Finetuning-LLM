WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM claims
    WHERE claim_type NOT IN ('medical', 'theft')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(approved_amount) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY claimed_amount ASC, id) AS claimed_amount_change
    FROM cleaned
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IN ('IN_REVIEW', 'OPEN')
              AND c.policy_id <= 9
        )
      AND s.claim_date_label IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(claim_date_label, '#', CAST(policy_id AS STRING)) AS claim_date_label_ref,
        FLOOR(claimed_amount_change / 50000) * 50000 AS claimed_amount_change_bucket
    FROM with_activity
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        YEAR(reported_at) AS reported_at_year,
        MIN(claim_date) AS first_claim_date,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM derived
    GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM'), YEAR(reported_at)
)
SELECT
    claim_date_ym,
    reported_at_year,
    first_claim_date,
    distinct_adjuster_ids
FROM rollup_base
WHERE claim_date_ym IS NOT NULL
ORDER BY first_claim_date NULLS LAST, distinct_adjuster_ids DESC
