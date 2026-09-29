WITH prepared_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        a.team AS adjuster_team
    FROM prepared_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id,
        claimed_amount - LAG(claimed_amount) OVER w_all_by_claim_date AS claimed_amount_change,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date NULLS LAST) AS claim_date_dense_rank,
        MAX(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY claim_date DESC, id) AS rn,
        LEAD(claimed_amount, 1, 0) OVER (PARTITION BY adjuster_team ORDER BY claim_date DESC, id) AS next_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id)) AS distinct_claim_type_in_adjuster_id,
        LAG(claim_date) OVER (PARTITION BY adjuster_team, adjuster_id ORDER BY claim_date, id) AS prev_claim_date,
        COUNT(*) OVER (PARTITION BY adjuster_team, policy_id) AS rows_in_adjuster_team_policy_id
    FROM with_details
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_all_by_claim_date AS (ORDER BY claim_date, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE rn = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(max_claimed_amount_so_far / NULLIF(SUM(max_claimed_amount_so_far) OVER (PARTITION BY claim_date_dense_rank), 0), 4) AS max_claimed_amount_so_far_share_of_claim_date_de,
        NTH_VALUE(rn, 2) OVER (PARTITION BY status, YEAR(claim_date) ORDER BY prev_claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rn,
        LEAD(claim_date) OVER (ORDER BY rn ASC, id) AS next_claim_date,
        LEAD(claim_date_dense_rank) OVER (PARTITION BY status, YEAR(claim_date) ORDER BY prev_claim_date, id) AS next_claim_date_dense_rank,
        COUNT(*) OVER w_claim_date_dense_rank AS rows_in_claim_date_dense_rank,
        LEAD(rn, 3) OVER w_status_claim_date_year_by_pr_2 AS next3_rn,
        LAST_VALUE(rn) IGNORE NULLS OVER (PARTITION BY status, YEAR(claim_date) ORDER BY prev_claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rn,
        NTILE(5) OVER (PARTITION BY status, YEAR(claim_date) ORDER BY prev_claim_date, id) AS prev_claim_date_quintile
    FROM leaders
    WINDOW w_claim_date_dense_rank AS (PARTITION BY claim_date_dense_rank), w_status_claim_date_year_by_pr_2 AS (PARTITION BY status, YEAR(claim_date) ORDER BY prev_claim_date, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    adjuster_team,
    rows_in_adjuster_id,
    claimed_amount_change,
    claim_date_dense_rank,
    max_claimed_amount_so_far,
    next_claimed_amount,
    distinct_claim_type_in_adjuster_id,
    prev_claim_date,
    rows_in_adjuster_team_policy_id,
    max_claimed_amount_so_far_share_of_claim_date_de,
    next_claim_date_dense_rank,
    next3_rn,
    last_known_rn
FROM with_windows
ORDER BY next3_rn
