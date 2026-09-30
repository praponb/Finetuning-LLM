WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE status = 'REJECTED'
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.claimed_amount_per_approved_amount,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        po.risk_score AS holder_risk_score,
        po.holder_name
    FROM cleaned s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(approved_amount) OVER (ORDER BY policy_start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_sum_7,
        MIN(approved_amount) OVER (PARTITION BY YEAR(policy_start_date), adjuster_id) AS policy_start_date_year_adjuster_id_min_approved_,
        SIZE(COLLECT_SET(policy_status) OVER w_adjuster_id) AS distinct_policy_status_in_adjuster_id,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_status) AS rows_in_adjuster_id_policy_status,
        COUNT(*) OVER (PARTITION BY YEAR(policy_start_date), adjuster_id) AS rows_in_policy_start_date_year_adjuster_id,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY adjuster_id, policy_status), 0), 4) AS holder_risk_score_share_of_adjuster_id_policy_st,
        RANK() OVER (PARTITION BY policy_holder_id, status ORDER BY policy_start_date) AS policy_start_date_rank,
        SUM(holder_risk_score) OVER (PARTITION BY YEAR(policy_start_date), adjuster_id) AS policy_start_date_year_adjuster_id_sum_holder_ri,
        MAX(holder_risk_score) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_holder_risk_score,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_status), 0), 4) AS approved_amount_share_of_adjuster_id_policy_stat,
        LEAD(policy_start_date, 3) OVER w_all_by_policy_start_date AS next3_policy_start_date,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY adjuster_id, policy_status), 2) AS holder_risk_score_vs_adjuster_id_policy_status_a,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_approved_amount
    FROM combined_details
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_all_by_policy_start_date AS (ORDER BY policy_start_date ASC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    status,
    claimed_amount_per_approved_amount,
    policy_holder_id,
    policy_start_date,
    policy_status,
    policy_start_date_year_adjuster_id_min_approved_,
    rows_in_adjuster_id_policy_status,
    rows_in_policy_start_date_year_adjuster_id,
    policy_start_date_rank,
    policy_start_date_year_adjuster_id_sum_holder_ri,
    adjuster_id_max_holder_risk_score,
    approved_amount_share_of_adjuster_id_policy_stat,
    next3_policy_start_date,
    holder_risk_score_vs_adjuster_id_policy_status_a,
    adjuster_id_avg_approved_amount
FROM windowed
ORDER BY holder_risk_score_vs_adjuster_id_policy_status_a DESC, policy_start_date_year_adjuster_id_sum_holder_ri
