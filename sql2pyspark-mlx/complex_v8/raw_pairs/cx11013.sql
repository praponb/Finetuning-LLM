WITH valid_claims AS (
    -- Scope the claims rows
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
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 11:00:00'
      AND adjuster_id >= 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_prefix,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY claimed_amount DESC) AS claimed_amount_rank,
        ROUND(CUME_DIST() OVER w_claim_date_month_policy_id_b_2, 3) AS claimed_amount_cume_dist,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_adjuster_id_claim_date_month, 0), 4) AS claimed_amount_share_of_adjuster_id_claim_date_m,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date, id) AS prev_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date)), 2) AS approved_amount_vs_adjuster_id_claim_date_month_
    FROM valid_claims
    WINDOW w_claim_date_month_policy_id_b_2 AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY claimed_amount DESC, id), w_adjuster_id_claim_date_month AS (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date))
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE claimed_amount > (SELECT AVG(claimed_amount) FROM sequenced)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount_rank NULLS LAST, id), 3) AS claimed_amount_rank_cume_dist,
        ROUND(claimed_amount_cume_dist - AVG(claimed_amount_cume_dist) OVER (PARTITION BY YEAR(reported_at), claimed_amount_rank), 2) AS claimed_amount_cume_dist_vs_reported_at_year_cla,
        ROUND(AVG(running_count) OVER w_reported_at_year_claimed_amo, 2) AS reported_at_year_claimed_amount_rank_avg_running
    FROM without_matches
    WINDOW w_reported_at_year_claimed_amo AS (PARTITION BY YEAR(reported_at), claimed_amount_rank)
)
SELECT
    id,
    claim_date,
    claim_type,
    claimed_amount,
    reported_at,
    claimed_amount_rank,
    claimed_amount_cume_dist,
    prev_approved_amount,
    approved_amount_vs_adjuster_id_claim_date_month_,
    claimed_amount_rank_cume_dist,
    claimed_amount_cume_dist_vs_reported_at_year_cla,
    reported_at_year_claimed_amount_rank_avg_running,
    DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at,
    INITCAP(TRIM(claim_type)) AS claim_type_clean
FROM with_windows
