WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE (claimed_amount > 4868.7 OR policy_id IS NOT NULL)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        status_ref,
        claimed_amount_filled,
        ROUND(claimed_amount_filled / NULLIF(claimed_amount, 0), 3) AS claimed_amount_filled_per_claimed_amount
    FROM cleaned_claims
    WHERE status IS NOT NULL
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at
    FROM scored s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status_ref,
        s.claimed_amount_filled_per_claimed_amount,
        s.last_claim_reported_at,
        a.level AS adjuster_level
    FROM with_counts s
    JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'E%'
)
SELECT
    adjuster_id,
    adjuster_level,
    GROUPING(adjuster_id) AS is_total_adjuster_id,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(last_claim_reported_at) AS first_last_claim_reported_at
FROM joined
GROUP BY GROUPING SETS ((adjuster_id, adjuster_level), (adjuster_id), ())
ORDER BY adjuster_id NULLS LAST, adjuster_level NULLS LAST
