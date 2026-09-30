WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE status NOT IN ('APPROVED', 'IN_REVIEW')
      AND approved_amount < 14668.0
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix_2
    FROM filtered_claims
)
SELECT *
FROM (
    SELECT policy_id, status, approved_amount
    FROM tagged
) src
PIVOT (
    MIN(approved_amount)
    FOR status IN ('OPEN', 'REJECTED')
)
ORDER BY policy_id
