WITH valid AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND status IN ('IN_REVIEW', 'APPROVED', 'OPEN')
),
stats AS (
    -- Group-level totals
    SELECT
        claim_type,
        ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount,
        COUNT(*) AS row_count,
        COUNT(DISTINCT status) AS distinct_status
    FROM valid
    WHERE policy_id >= 12
    GROUP BY claim_type
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE row_count > (SELECT MIN(row_count) FROM stats)
),
labeled AS (
    -- Add calculated columns
    SELECT
        claim_type,
        stddev_claimed_amount,
        row_count,
        distinct_status,
        COALESCE(distinct_status, 0) AS distinct_status_filled
    FROM qualified
)
SELECT claim_type, field, value
FROM (
    SELECT claim_type, distinct_status, distinct_status_filled
    FROM labeled
) src
UNPIVOT (
    value FOR field IN (distinct_status, distinct_status_filled)
) unp
