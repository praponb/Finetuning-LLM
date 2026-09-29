WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(claim_type_or_unknown)) AS claim_type_or_unknown_len,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM scoped_claims
),
claim_type_stats AS (
    -- Avg claim_type_len per claim_type
    SELECT claim_type, AVG(claim_type_len) AS avg_claim_type_len
    FROM labeled
    GROUP BY claim_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_type_len,
        ROUND(s.claim_type_len - g.avg_claim_type_len, 2) AS claim_type_len_minus_avg
    FROM labeled s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claim_type_len > g.avg_claim_type_len
),
without_matches AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level BETWEEN 1 AND 3
              AND a.adjuster_name IS NOT NULL
        )
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.team AS adjuster_team
    FROM without_matches s
    JOIN adjusters a ON a.id = s.adjuster_id AND LOWER(a.adjuster_name) LIKE '%a %'
),
labeled_2 AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d
    FROM combined_details_adjuster
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    least_approved_amount_claimed_amount,
    claim_type_or_unknown,
    claim_type_len,
    claim_type_or_unknown_len,
    claim_date_label,
    avg_claim_type_len,
    claim_type_len_minus_avg,
    adjuster_team,
    claim_type_clean,
    claimed_amount_bucket,
    GREATEST(claimed_amount, claim_type_len_minus_avg) AS greatest_claimed_amount_claim_type_len_minus_avg
FROM labeled_2
ORDER BY claim_type_or_unknown_len DESC, claim_date_label NULLS LAST
