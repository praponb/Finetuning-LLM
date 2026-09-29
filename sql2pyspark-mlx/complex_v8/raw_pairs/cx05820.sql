WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM claims
    WHERE approved_amount <= 25402.3
      AND claim_type <> 'fire'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        approved_amount - LAG(approved_amount) OVER w_policy_id_by_reported_at AS approved_amount_change,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_claimed_amount,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date_month_end DESC, id), 3) AS claim_date_month_end_cume_dist,
        DENSE_RANK() OVER (PARTITION BY policy_id, status ORDER BY approved_amount, id) AS approved_amount_dense_rank,
        claimed_amount - LAG(claimed_amount) OVER w_all_by_claim_date_month_end AS claimed_amount_change,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id, 0), 4) AS claimed_amount_share_of_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        RANK() OVER w_policy_id_status_by_approved AS approved_amount_rank,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status)) AS distinct_adjuster_id_in_status,
        ROUND(CUME_DIST() OVER w_status_adjuster_id_by_claime, 3) AS claimed_amount_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id, status ORDER BY approved_amount NULLS LAST, id), 3) AS approved_amount_percent_rank,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status), 2) AS claimed_amount_vs_status_avg,
        MAX(claimed_amount_filled) OVER (PARTITION BY policy_id, status ORDER BY approved_amount, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_filled_max_1_prior
    FROM valid
    WINDOW w_policy_id_by_reported_at AS (PARTITION BY policy_id ORDER BY reported_at DESC, id), w_all_by_claim_date_month_end AS (ORDER BY claim_date_month_end DESC, id), w_policy_id AS (PARTITION BY policy_id), w_policy_id_status_by_approved AS (PARTITION BY policy_id, status ORDER BY approved_amount, id), w_status_adjuster_id_by_claime AS (PARTITION BY status, adjuster_id ORDER BY claimed_amount, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    claimed_amount_filled,
    claim_date_month_end,
    claimed_amount_change,
    claimed_amount_vs_policy_id_avg,
    second_claim_date,
    approved_amount_rank,
    distinct_adjuster_id_in_status,
    approved_amount_percent_rank,
    claimed_amount_vs_status_avg,
    claimed_amount_filled_max_1_prior
FROM windowed
WHERE status IN ('OPEN', 'APPROVED')
ORDER BY second_claim_date, claimed_amount_vs_policy_id_avg NULLS LAST
