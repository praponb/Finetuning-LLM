WITH base_claims AS (
    -- Keep valid rows and derive helper columns
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
        ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj
    FROM claims
    WHERE (claim_type = 'delay' OR adjuster_id >= 6)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROUND(PERCENT_RANK() OVER w_status_adjuster_id_by_claime, 3) AS claimed_amount_adj_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at), 3) AS reported_at_percent_rank,
        MAX(approved_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_max_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER w_status_adjuster_id, 2) AS approved_amount_vs_status_adjuster_id_avg,
        RANK() OVER (PARTITION BY adjuster_id, status ORDER BY reported_at, id) AS reported_at_rank,
        SUM(claimed_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_sum_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_max_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_sum_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_status_policy_id, 0), 4) AS claimed_amount_share_of_status_policy_id,
        ROUND(AVG(claimed_amount_adj) OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount_adj, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_adj_avg_4,
        COUNT(*) OVER w_status_adjuster_id AS rows_in_status_adjuster_id,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status, policy_id), 0), 4) AS approved_amount_share_of_status_policy_id,
        LEAD(approved_amount, 2) OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount_adj, id) AS next2_approved_amount,
        NTILE(10) OVER w_all_by_claim_date AS claim_date_decile
    FROM base_claims
    WINDOW w_status_adjuster_id_by_claime AS (PARTITION BY status, adjuster_id ORDER BY claimed_amount_adj, id), w_status_adjuster_id AS (PARTITION BY status, adjuster_id), w_status_policy_id AS (PARTITION BY status, policy_id), w_all_by_claim_date AS (ORDER BY claim_date DESC NULLS FIRST, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_date_decile,
        ROUND(100 * COUNT(CASE WHEN status NOT IN ('IN_REVIEW', 'OPEN') THEN 1 END) / COUNT(*), 1) AS pct_status_not_in_in_review_open,
        SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS n_status_open,
        COUNT(*) AS cnt
    FROM windowed
    GROUP BY 1
    HAVING AVG(reported_at_percent_rank) > 0.1
)
SELECT
    claim_date_decile,
    n_status_open,
    cnt,
    ABS(claim_date_decile - 4) AS claim_date_decile_distance,
    COALESCE(n_status_open, 0) AS n_status_open_filled
FROM stats
ORDER BY cnt DESC, n_status_open, claim_date_decile
LIMIT 10
