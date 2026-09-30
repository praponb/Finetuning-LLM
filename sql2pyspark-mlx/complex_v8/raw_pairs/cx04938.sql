WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE claim_type NOT IN ('medical', 'delay')
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_claims
    WHERE claimed_amount > (SELECT AVG(claimed_amount) FROM prepared_claims)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        status_len,
        adjuster_id,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MAX(claim_date) AS last_claim_date
    FROM qualified
    GROUP BY status_len, adjuster_id
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE status_len > (SELECT AVG(status_len) FROM totals)
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.adjuster_name
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level BETWEEN 2 AND 3
),
annotated AS (
    -- Derive helper columns
    SELECT
        status_len,
        adjuster_id,
        claim_type_list,
        adjuster_name,
        COALESCE(status_len, 0) AS status_len_filled
    FROM with_parent_adjuster
),
joined AS (
    -- Join adjusters
    SELECT
        s.status_len,
        s.adjuster_id,
        s.claim_type_list,
        s.adjuster_name,
        s.status_len_filled,
        a.adjuster_name AS adjuster_adjuster_name,
        a.team AS adjuster_team
    FROM annotated s
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    status_len,
    claim_type_list,
    adjuster_name,
    status_len_filled,
    adjuster_adjuster_name,
    adjuster_team
FROM joined
ORDER BY status_len_filled, adjuster_adjuster_name
