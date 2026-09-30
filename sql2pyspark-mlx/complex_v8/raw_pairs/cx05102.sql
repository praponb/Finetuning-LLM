WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        QUARTER(claim_date) AS claim_date_quarter,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(claim_date_quarter - 2) AS claim_date_quarter_distance,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ROUND(status_len / NULLIF(claim_date_quarter, 0), 3) AS status_len_per_claim_date_quarter
    FROM valid
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT s.*, a.level AS adjuster_level
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level < 3
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details_adjuster s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.policy_id <= 27
              AND c.reported_at >= TIMESTAMP '2025-01-31 14:00:00'
        )
      AND s.reported_at >= TIMESTAMP '2025-01-25 13:00:00'
)
SELECT
    id,
    adjuster_id,
    claim_date,
    approved_amount,
    status,
    status_len,
    claim_date_quarter_distance,
    status_or_unknown,
    status_len_per_claim_date_quarter,
    adjuster_level
FROM qualified
ORDER BY adjuster_id DESC, id DESC
