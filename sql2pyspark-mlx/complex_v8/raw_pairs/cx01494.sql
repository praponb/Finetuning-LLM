WITH scoped AS (
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
        reported_at,
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d
    FROM claims
    WHERE status <> 'IN_REVIEW'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_date_plus_365d,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date)), 2) AS approved_amount_vs_policy_id_claim_date_month_av,
        SUM(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)) AS reported_at_to_date_sum_approved_amount,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at)) AS rows_in_reported_at_to_date,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        MIN(claimed_amount) OVER w_reported_at_to_date AS reported_at_to_date_min_claimed_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)), 0), 4) AS approved_amount_share_of_reported_at_to_date,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id), 3) AS reported_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY approved_amount, id) AS approved_amount_seq,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY TO_DATE(reported_at))) AS distinct_policy_id_in_reported_at_to_date,
        RANK() OVER (ORDER BY claim_date ASC) AS claim_date_rank,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_id_claim_date_month, 2) AS claimed_amount_vs_policy_id_claim_date_month_avg,
        SUM(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date)) AS policy_id_claim_date_month_sum_approved_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS claimed_amount_vs_reported_at_to_date_avg,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM scoped
    WINDOW w_reported_at_to_date AS (PARTITION BY TO_DATE(reported_at)), w_policy_id_claim_date_month AS (PARTITION BY policy_id, MONTH(claim_date))
),
summary AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        approved_amount_seq,
        policy_id,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        COUNT(*) FILTER (WHERE status = 'REJECTED') AS n_status_rejected,
        SUM(CASE WHEN status = 'IN_REVIEW' THEN reported_at_to_date_min_claimed_amount ELSE 0 END) AS reported_at_to_date_min_claimed_amount_status_in,
        SUM(first_known_claimed_amount) FILTER (WHERE status <> 'PAID') AS first_known_claimed_amount_status_paid,
        MAX(claim_date_plus_365d) AS last_claim_date_plus_365d,
        ROUND(SUM(reported_at_to_date_sum_approved_amount) / NULLIF(SUM(approved_amount), 0), 3) AS reported_at_to_date_sum_approved_amount_to_appro
    FROM ranked
    GROUP BY adjuster_id, approved_amount_seq, policy_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY first_known_claimed_amount_status_paid ASC, adjuster_id, approved_amount_seq, policy_id) AS rn,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, approved_amount_seq)) AS distinct_policy_id_in_adjuster_id_approved_amoun,
        ROUND(AVG(n_status_rejected) OVER (PARTITION BY adjuster_id, approved_amount_seq), 2) AS adjuster_id_approved_amount_seq_avg_n_status_rej,
        pct_status_is_not_null - LAG(pct_status_is_not_null) OVER (PARTITION BY policy_id ORDER BY first_known_claimed_amount_status_paid ASC, adjuster_id, approved_amount_seq, policy_id) AS pct_status_is_not_null_change
    FROM summary
)
-- Final projection
SELECT
    adjuster_id,
    approved_amount_seq,
    policy_id,
    n_status_rejected,
    reported_at_to_date_min_claimed_amount_status_in,
    first_known_claimed_amount_status_paid,
    last_claim_date_plus_365d,
    reported_at_to_date_sum_approved_amount_to_appro,
    rn,
    distinct_policy_id_in_adjuster_id_approved_amoun,
    adjuster_id_approved_amount_seq_avg_n_status_rej
FROM windowed
WHERE pct_status_is_not_null > 10.0
ORDER BY adjuster_id_approved_amount_seq_avg_n_status_rej NULLS LAST, policy_id DESC, adjuster_id, approved_amount_seq
LIMIT 20
