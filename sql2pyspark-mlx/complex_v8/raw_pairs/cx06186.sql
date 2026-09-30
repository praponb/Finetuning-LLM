WITH prepared AS (
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
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE policy_id <= 4
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        s.least_approved_amount_claimed_amount,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM prepared s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        least_approved_amount_claimed_amount,
        NTILE(5) OVER (PARTITION BY claim_type, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at, id) AS reported_at_quintile,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_team, policy_id) AS adjuster_team_policy_id_max_claimed_amount,
        LAG(reported_at, 2) OVER w_claim_type_claim_date_month AS prev2_reported_at,
        claimed_amount - LAG(claimed_amount) OVER w_claim_type_claim_date_month AS claimed_amount_change,
        LAST_VALUE(claim_date) OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_team, policy_id), 0), 4) AS claimed_amount_share_of_adjuster_team_policy_id
    FROM with_details
    WINDOW w_claim_type_claim_date_month AS (PARTITION BY claim_type, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at, id)
),
latest AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        least_approved_amount_claimed_amount,
        reported_at_quintile,
        adjuster_team_policy_id_max_claimed_amount,
        prev2_reported_at,
        claimed_amount_change,
        last_claim_date,
        claimed_amount_share_of_adjuster_team_policy_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY last_claim_date DESC, id DESC) AS rn
        FROM with_windows
    ) ranked
    WHERE rn = 1
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(reported_at_quintile) OVER (PARTITION BY adjuster_id, policy_id), 2) AS adjuster_id_policy_id_avg_reported_at_quintile,
        ROUND(AVG(reported_at_quintile) OVER (ORDER BY claimed_amount_change, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS reported_at_quintile_avg_2,
        ROW_NUMBER() OVER w_all_by_claimed_amount_change_2 AS rn,
        ROUND(PERCENT_RANK() OVER w_all_by_claimed_amount_change_2, 3) AS claimed_amount_change_percent_rank
    FROM latest
    WINDOW w_all_by_claimed_amount_change_2 AS (ORDER BY claimed_amount_change, id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.least_approved_amount_claimed_amount,
        s.reported_at_quintile,
        s.adjuster_team_policy_id_max_claimed_amount,
        s.claimed_amount_change,
        s.last_claim_date,
        s.claimed_amount_share_of_adjuster_team_policy_id,
        s.adjuster_id_policy_id_avg_reported_at_quintile,
        s.reported_at_quintile_avg_2,
        s.rn,
        a.adjuster_name
    FROM with_windows_2 s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    reported_at_quintile,
    adjuster_team_policy_id_max_claimed_amount,
    claimed_amount_change,
    last_claim_date,
    claimed_amount_share_of_adjuster_team_policy_id,
    adjuster_id_policy_id_avg_reported_at_quintile,
    reported_at_quintile_avg_2,
    rn,
    adjuster_name
FROM combined_details
ORDER BY claimed_amount_share_of_adjuster_team_policy_id
