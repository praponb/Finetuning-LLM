WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claimed_amount >= 19609.8
      AND claim_date BETWEEN DATE '2024-04-27' AND DATE '2025-10-15'
),
all_events AS (
    -- Stack the branches
    SELECT
        'claim' AS source,
        claim_type,
        reported_at AS event_time,
        adjuster_id AS entity_id
    FROM valid_claims
    UNION ALL
    SELECT
        'claim_2' AS source,
        claim_type,
        reported_at AS event_time,
        id AS entity_id
    FROM claims
    WHERE status IN ('REJECTED', 'IN_REVIEW')
    UNION ALL
    SELECT
        'claim_3' AS source,
        status AS claim_type,
        reported_at AS event_time,
        id AS entity_id
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claimed_amount <= 30115.9
)
SELECT source, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY source
ORDER BY source
