WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        CASE WHEN approved_amount >= 14377.6 THEN 'HIGH' WHEN approved_amount >= 1364.5 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE approved_amount BETWEEN 0.0 AND 23487.5
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.approved_amount_tier,
        s.claim_type_len,
        a.team AS adjuster_team
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(claim_type_len - 9) AS claim_type_len_distance,
        DATE_ADD(claim_date, 90) AS claim_date_plus_90d,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM with_parent_adjuster
    WHERE claim_type_len <= 11
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type_len,
        adjuster_team,
        LAG(claim_date) OVER (PARTITION BY status ORDER BY claim_date_plus_90d DESC, id) AS prev_claim_date,
        MAX(claim_type_len) OVER (PARTITION BY policy_id, claim_type_len ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS claim_type_len_max_6_prior,
        ROUND(claim_type_len_distance / NULLIF(SUM(claim_type_len_distance) OVER (PARTITION BY status), 0), 4) AS claim_type_len_distance_share_of_status,
        ROUND(claim_type_len / NULLIF(SUM(claim_type_len) OVER (PARTITION BY status), 0), 4) AS claim_type_len_share_of_status
    FROM enriched
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS claim_status_count,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.min_claim_claimed_amount,
        c.last_claim_date
    FROM sequenced s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type_len,
    adjuster_team,
    prev_claim_date,
    claim_type_len_max_6_prior,
    claim_type_len_distance_share_of_status,
    min_claim_claimed_amount,
    last_claim_date
FROM summarized_rows
ORDER BY prev_claim_date DESC, adjuster_team
