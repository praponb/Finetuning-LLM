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
        ABS(approved_amount - 9608.4) AS approved_amount_distance
    FROM claims
    WHERE claim_type IS NOT NULL
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM cleaned_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        adjuster_level,
        adjuster_name,
        LAG(adjuster_level) OVER w_adjuster_team_policy_id_by_c AS prev_adjuster_level,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY adjuster_level, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_7,
        MAX(adjuster_level) OVER (PARTITION BY policy_id, adjuster_team) AS policy_id_adjuster_team_max_adjuster_level,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_policy_id, 0), 4) AS adjuster_level_share_of_policy_id,
        LAST_VALUE(adjuster_level) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_adjuster_level,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date DESC, id)) AS days_since_prev_claim_date,
        LEAD(adjuster_level, 2) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id) AS next2_adjuster_level,
        ROW_NUMBER() OVER (PARTITION BY adjuster_team, policy_id ORDER BY claim_date DESC, id) AS seq_no,
        RANK() OVER w_all_by_claim_date_ties AS claim_date_rank,
        RANK() OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date DESC, id) AS claim_date_rank_2
    FROM combined_details
    WINDOW w_adjuster_team_policy_id_by_c AS (PARTITION BY adjuster_team, policy_id ORDER BY claim_date DESC, id), w_policy_id AS (PARTITION BY policy_id), w_all_by_claim_date_ties AS (ORDER BY claim_date DESC)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        adjuster_name,
        prev_adjuster_level,
        adjuster_level_avg_7,
        policy_id_adjuster_team_max_adjuster_level,
        adjuster_level_share_of_policy_id,
        last_adjuster_level,
        days_since_prev_claim_date,
        seq_no,
        claim_date_rank,
        MONTH(claim_date) AS claim_date_month,
        CONCAT(adjuster_name, '#', CAST(id AS STRING)) AS adjuster_name_ref,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM with_windows
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        adjuster_name,
        prev_adjuster_level,
        adjuster_level_avg_7,
        policy_id_adjuster_team_max_adjuster_level,
        adjuster_level_share_of_policy_id,
        seq_no,
        claim_date_rank,
        claim_date_month,
        adjuster_name_ref,
        days_since_claim_date,
        ROUND(claim_date_rank / NULLIF(SUM(claim_date_rank) OVER w_adjuster_id_claim_date_month, 0), 4) AS claim_date_rank_share_of_adjuster_id_claim_date_,
        ROW_NUMBER() OVER w_adjuster_id_claim_date_month_2 AS seq_no_2,
        ROUND(AVG(last_adjuster_level) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date)), 2) AS adjuster_id_claim_date_month_avg_last_adjuster_l,
        LEAD(seq_no, 2, 0) OVER (PARTITION BY adjuster_id, claim_date_month ORDER BY last_adjuster_level DESC, id) AS next2_seq_no,
        MAX(days_since_prev_claim_date) OVER (PARTITION BY adjuster_id, claim_date_month ORDER BY policy_id_adjuster_team_max_adjuster_level DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_prev_claim_date_max_3_prior
    FROM derived
    WINDOW w_adjuster_id_claim_date_month AS (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date)), w_adjuster_id_claim_date_month_2 AS (PARTITION BY adjuster_id, claim_date_month ORDER BY policy_id_adjuster_team_max_adjuster_level DESC, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    adjuster_name,
    adjuster_level_avg_7,
    policy_id_adjuster_team_max_adjuster_level,
    adjuster_level_share_of_policy_id,
    claim_date_rank,
    claim_date_month,
    days_since_claim_date,
    adjuster_id_claim_date_month_avg_last_adjuster_l,
    next2_seq_no,
    days_since_prev_claim_date_max_3_prior
FROM sequenced
WHERE adjuster_level_share_of_policy_id <= 0.8
