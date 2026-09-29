WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE (reported_at < TIMESTAMP '2025-02-18 09:00:00' OR status = 'REJECTED')
),
joined_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.claimed_amount_minus_approved_amount,
        a.team AS adjuster_team
    FROM cleaned_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team IN ('Property', 'Motor')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(reported_at) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount DESC, id) AS next_reported_at,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_id, 2) AS claimed_amount_vs_policy_id_avg,
        MIN(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_min_claimed_amount,
        FIRST_VALUE(reported_at) OVER (ORDER BY claimed_amount_minus_approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        LAG(reported_at) OVER w_policy_id_claim_date_month_b AS prev_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount DESC), 3) AS claimed_amount_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_cume_dist,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id), 0), 4) AS claimed_amount_share_of_policy_id,
        ROW_NUMBER() OVER (ORDER BY reported_at, id) AS rn,
        LAG(claim_date, 2) OVER (ORDER BY reported_at, id) AS prev2_claim_date,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claimed_amount_minus_approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount_minus_approved_amount NULLS LAST, id), 3) AS claimed_amount_minus_approved_amount_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount_minus_approved_amount NULLS LAST), 3) AS claimed_amount_minus_approved_amount_percent_ran
    FROM joined_adjuster
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_policy_id_claim_date_month_b AS (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount DESC, id)
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claimed_amount_minus_approved_amount,
    adjuster_team,
    claimed_amount_vs_policy_id_avg,
    policy_id_min_claimed_amount,
    prev_reported_at,
    claimed_amount_percent_rank,
    claimed_amount_share_of_policy_id,
    prev2_claim_date,
    second_reported_at,
    claimed_amount_minus_approved_amount_cume_dist,
    claimed_amount_minus_approved_amount_percent_ran,
    LEAST(rn, claimed_amount) AS least_rn_claimed_amount
FROM ranked
ORDER BY adjuster_id, claimed_amount_minus_approved_amount_cume_dist DESC NULLS FIRST, id
LIMIT 10
