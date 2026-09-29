WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label,
        CASE WHEN status IN ('PAID', 'REJECTED') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
        DATE_TRUNC('HOUR', reported_at) AS reported_at_hour
    FROM claims
    WHERE policy_id <= 3
      AND claim_type IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        approved_amount,
        reported_at,
        claim_type_status_label,
        status_group,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
    FROM filtered
    WHERE reported_at_hour < TIMESTAMP '2025-02-05 09:00:00'
)
SELECT *
FROM (
    SELECT adjuster_id, status_group, approved_amount
    FROM scored
) src
PIVOT (
    SUM(approved_amount)
    FOR status_group IN ('group_a', 'group_b', 'other')
)
ORDER BY adjuster_id
