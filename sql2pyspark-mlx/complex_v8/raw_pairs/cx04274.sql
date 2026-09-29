WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type_or_unknown ORDER BY claimed_amount ASC), 3) AS claimed_amount_percent_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_approved_amount_ties, 3) AS approved_amount_percent_rank,
        MAX(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_max_claimed_amount
    FROM cleaned
    WINDOW w_all_by_approved_amount_ties AS (ORDER BY approved_amount ASC)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.adjuster_name,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM with_trends s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    reported_at,
    approved_amount_per_claimed_amount,
    claim_type_or_unknown,
    claimed_amount_percent_rank,
    policy_status,
    policy_start_date,
    ABS(approved_amount - 14217.6) AS approved_amount_distance
FROM joined
ORDER BY claimed_amount DESC, reported_at DESC, id
LIMIT 10
