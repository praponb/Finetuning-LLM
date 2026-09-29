WITH filtered AS (
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
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, status), 2) AS claimed_amount_vs_adjuster_id_status_avg,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status, policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_1_prior,
        SUM(claimed_amount) OVER (PARTITION BY claim_type, TO_DATE(claim_date_week) ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_4,
        ROUND(AVG(approved_amount_bucket) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date_week DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_bucket_avg_1_prior,
        RANK() OVER (ORDER BY claimed_amount NULLS LAST) AS claimed_amount_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type, TO_DATE(claim_date_week) ORDER BY approved_amount), 3) AS approved_amount_percent_rank,
        LAST_VALUE(approved_amount) OVER (ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS adjuster_id_policy_id_avg_approved_amount,
        NTILE(10) OVER (ORDER BY claimed_amount NULLS LAST, id) AS claimed_amount_decile,
        NTILE(5) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date_week DESC, id) AS claim_date_week_quintile,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, status), 2) AS approved_amount_vs_adjuster_id_status_avg,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS adjuster_id_policy_id_avg_claimed_amount,
        SUM(approved_amount) OVER (PARTITION BY status, policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_sum_3
    FROM filtered
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        claim_date_week,
        approved_amount_bucket,
        claimed_amount_vs_adjuster_id_status_avg,
        claimed_amount_avg_1_prior,
        claimed_amount_sum_4,
        approved_amount_bucket_avg_1_prior,
        approved_amount_percent_rank,
        last_approved_amount,
        adjuster_id_policy_id_avg_approved_amount,
        claimed_amount_decile,
        claim_date_week_quintile,
        approved_amount_vs_adjuster_id_status_avg,
        adjuster_id_policy_id_avg_claimed_amount,
        approved_amount_sum_3
    FROM with_windows
    WHERE claimed_amount_rank <= 2
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    status,
    claimed_amount_vs_adjuster_id_status_avg,
    claimed_amount_avg_1_prior,
    claimed_amount_sum_4,
    approved_amount_bucket_avg_1_prior,
    approved_amount_percent_rank,
    adjuster_id_policy_id_avg_approved_amount,
    claimed_amount_decile,
    claim_date_week_quintile,
    approved_amount_vs_adjuster_id_status_avg,
    adjuster_id_policy_id_avg_claimed_amount,
    approved_amount_sum_3,
    FLOOR(claimed_amount_sum_4 / 50000) * 50000 AS claimed_amount_sum_4_bucket,
    CASE WHEN claimed_amount >= 29359.4 THEN 'A' WHEN claimed_amount >= 20436.5 THEN 'B' ELSE 'C' END AS claimed_amount_tier
FROM top_rows
ORDER BY approved_amount_bucket_avg_1_prior DESC, claimed_amount_decile DESC, id
LIMIT 20
