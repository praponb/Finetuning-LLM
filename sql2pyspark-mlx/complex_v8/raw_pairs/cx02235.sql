WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        QUARTER(claim_date) AS claim_date_quarter,
        ADD_MONTHS(claim_date, 1) AS claim_date_plus_1m
    FROM claims
    WHERE claim_type = 'delay'
      AND policy_id >= 6
),
latest AS (
    -- Keep the latest row per adjuster_id, status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        claim_date_quarter,
        claim_date_plus_1m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, status ORDER BY claim_date_quarter DESC, id DESC) AS rn
        FROM prepared_claims
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(claim_date_quarter, 0) AS claim_date_quarter_filled,
        TO_DATE(reported_at) AS reported_at_date,
        CASE WHEN status IN ('REJECTED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM latest
    WHERE claim_date_plus_1m >= DATE '2024-05-01'
)
SELECT id, metric_name, reading
FROM (
    SELECT id, claim_date_quarter, claim_date_quarter_filled
    FROM enriched
) src
UNPIVOT (
    reading FOR metric_name IN (claim_date_quarter AS m_claim_date_quarter, claim_date_quarter_filled AS claim_date_quarter_filled_v)
) unp
