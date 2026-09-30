WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE status <> 'PAID'
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM scoped_claims s
    JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_1_prior,
        SUM(approved_amount) OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_holder_id_status_or_u, 2) AS claimed_amount_vs_policy_holder_id_status_or_unk,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id) AS claim_date_seq,
        SUM(approved_amount) OVER (PARTITION BY policy_status, YEAR(claim_date)) AS policy_status_claim_date_year_sum_approved_amoun,
        NTILE(10) OVER (ORDER BY policy_start_date DESC NULLS FIRST, id) AS policy_start_date_decile,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_holder_id, status_or_unknown), 0), 4) AS approved_amount_share_of_policy_holder_id_status,
        LAG(approved_amount, 3) OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id) AS prev3_approved_amount
    FROM with_parent_policy
    WINDOW w_policy_holder_id_status_or_u AS (PARTITION BY policy_holder_id, status_or_unknown)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(approved_amount, 2) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_vs_policy_holder_id_status_or_unk DESC, id) AS next2_approved_amount,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_vs_policy_holder_id_status_or_unk DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        prev3_approved_amount - LAG(prev3_approved_amount) OVER w_policy_start_date_month_clai AS prev3_approved_amount_change,
        NTH_VALUE(claimed_amount_avg_1_prior, 2) OVER (PARTITION BY MONTH(policy_start_date), DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount_avg_1_prior,
        ROUND(claimed_amount_avg_1_prior - AVG(claimed_amount_avg_1_prior) OVER (PARTITION BY adjuster_id, policy_id), 2) AS claimed_amount_avg_1_prior_vs_adjuster_id_policy,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY MONTH(policy_start_date), DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount NULLS LAST, id) AS approved_amount_change,
        ROUND(AVG(claimed_amount_avg_1_prior) OVER (PARTITION BY MONTH(policy_start_date), DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_1_prior_avg_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_vs_policy_holder_id_status_or_unk DESC, id), 3) AS claimed_amount_vs_policy_holder_id_status_or_unk_2
    FROM metrics
    WINDOW w_policy_start_date_month_clai AS (PARTITION BY MONTH(policy_start_date), DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    status,
    status_or_unknown,
    policy_holder_id,
    policy_start_date,
    policy_status_claim_date_year_sum_approved_amoun,
    policy_start_date_decile,
    approved_amount_share_of_policy_holder_id_status,
    prev3_approved_amount,
    next2_approved_amount,
    second_approved_amount,
    prev3_approved_amount_change,
    approved_amount_change,
    claimed_amount_avg_1_prior_avg_4,
    DATEDIFF(claim_date, policy_start_date) AS days_policy_start_date_to_claim_date,
    LEAST(claim_date_seq, next2_approved_amount) AS least_claim_date_seq_next2_approved_amount
FROM windowed
WHERE status_or_unknown IS NOT NULL
ORDER BY least_claim_date_seq_next2_approved_amount, claimed_amount DESC
