WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claimed_amount < 42447.2
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FILTER (WHERE c.status IN ('PAID', 'REJECTED')) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_status_count,
        (SELECT COUNT(*) FILTER (WHERE c.status <> 'APPROVED') FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_status_count_2
    FROM recent_claims s
),
status_stats AS (
    -- Max claim_status_count_2 per status
    SELECT status, MAX(claim_status_count_2) AS max_claim_status_count_2
    FROM with_stats
    GROUP BY status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claim_status_count_2,
        ROUND(s.claim_status_count_2 - g.max_claim_status_count_2, 2) AS claim_status_count_2_minus_max
    FROM with_stats s
    JOIN status_stats g ON g.status = s.status
    WHERE s.claim_status_count_2 = g.max_claim_status_count_2
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM compared
    WHERE max_claim_status_count_2 < (SELECT MAX(max_claim_status_count_2) FROM compared)
)
SELECT
    claim_status_count,
    claim_count,
    adjuster_id,
    GROUPING(claim_status_count) AS is_total_claim_status_count,
    SUM(claim_status_count_2) AS total_claim_status_count_2
FROM without_matches
GROUP BY GROUPING SETS ((claim_status_count, claim_count, adjuster_id), (claim_status_count), ())
