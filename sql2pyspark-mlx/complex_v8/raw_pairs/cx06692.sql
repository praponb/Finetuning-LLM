WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-03-02'
      AND claim_type = 'delay'
),
joined_policy AS (
    -- Attach policy attributes
    SELECT s.*, p.start_date AS policy_start_date
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id AND p.end_date >= DATE '2026-01-23'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        claim_type_slug,
        policy_start_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY policy_id, status ORDER BY reported_at, id)) AS days_since_prev_claim_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status, adjuster_id), 2) AS claimed_amount_vs_status_adjuster_id_avg,
        LAG(claim_date) OVER (ORDER BY reported_at, id) AS prev_claim_date,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY policy_start_date DESC, id), 3) AS policy_start_date_percent_rank,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        FIRST_VALUE(policy_start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_start_date,
        MAX(claimed_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_max_claimed_amount,
        LEAD(claimed_amount, 3) OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claimed_amount DESC, id) AS next3_claimed_amount
    FROM joined_policy
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', second_claim_date) AS days_since_second_claim_date
    FROM with_trends
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(days_since_second_claim_date, 2) OVER (ORDER BY first_known_policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_second_claim_date,
        RANK() OVER (PARTITION BY status, DATE_TRUNC('MONTH', claim_date) ORDER BY second_claim_date) AS second_claim_date_rank,
        SUM(max_claimed_amount_so_far) OVER (ORDER BY policy_start_date_percent_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_claimed_amount_so_far_sum_6_prior,
        ROUND(max_claimed_amount_so_far / NULLIF(SUM(max_claimed_amount_so_far) OVER w_first_known_policy_start_dat, 0), 4) AS max_claimed_amount_so_far_share_of_first_known_p
    FROM scored
    WINDOW w_first_known_policy_start_dat AS (PARTITION BY MONTH(first_known_policy_start_date))
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    reported_at,
    policy_start_date,
    days_since_prev_claim_date,
    second_claim_date,
    status_adjuster_id_max_claimed_amount,
    next3_claimed_amount,
    second_days_since_second_claim_date,
    max_claimed_amount_so_far_share_of_first_known_p,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM ranked
ORDER BY claimed_amount DESC NULLS FIRST
