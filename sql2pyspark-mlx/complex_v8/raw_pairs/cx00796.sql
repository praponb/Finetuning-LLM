WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        INITCAP(TRIM(status)) AS status_clean
    FROM claims
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND TO_DATE(s.reported_at) < DATE '2025-02-06'
),
with_parent AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.status_clean,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM with_activity s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE LENGTH(TRIM(a.adjuster_name)) >= 4
),
latest AS (
    -- Keep the latest row per adjuster_team, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        status_clean,
        adjuster_name,
        adjuster_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team, adjuster_id ORDER BY reported_at DESC, id DESC) AS recency_rank
        FROM with_parent
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at,
        adjuster_name,
        adjuster_team,
        SUM(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id) AS policy_id_adjuster_id_sum_claimed_amount,
        NTILE(10) OVER (ORDER BY claimed_amount ASC, id) AS claimed_amount_decile,
        DENSE_RANK() OVER w_all_by_claimed_amount AS claimed_amount_dense_rank,
        LEAD(claimed_amount) OVER (ORDER BY claimed_amount ASC, id) AS next_claimed_amount
    FROM latest
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id)
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.adjuster_team,
        s.claimed_amount_decile,
        s.claimed_amount_dense_rank,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount
    FROM windowed s
    JOIN claims c ON c.policy_id = s.policy_id AND c.claimed_amount < 28900.9
),
with_activity_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM detail_rows_claims
    WHERE claim_claimed_amount > (SELECT AVG(claim_claimed_amount) FROM detail_rows_claims)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, adjuster_team, claim_approved_amount
    FROM with_activity_2
) src
PIVOT (
    AVG(claim_approved_amount)
    FOR adjuster_team IN ('Health', 'Motor', 'Travel')
)
ORDER BY policy_id
