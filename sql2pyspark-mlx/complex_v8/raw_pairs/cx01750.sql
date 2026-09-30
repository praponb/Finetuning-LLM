WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM claims
    WHERE (adjuster_id <= 2 OR claimed_amount >= 18924.4)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.claim_date_week,
        p.status AS policy_status,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id
    FROM filtered s
    LEFT JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount ASC, id), 3) AS claimed_amount_cume_dist,
        LEAD(reported_at) OVER w_all_by_claimed_amount AS next_reported_at,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_status, policy_id), 2) AS claimed_amount_vs_policy_status_policy_id_avg,
        COUNT(*) OVER (PARTITION BY policy_status, policy_id) AS rows_in_policy_status_policy_id,
        SUM(claimed_amount) OVER (ORDER BY policy_start_date ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_2,
        FIRST_VALUE(policy_start_date) OVER (ORDER BY policy_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_policy_start_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount ASC), 3) AS claimed_amount_percent_rank,
        MAX(claimed_amount) OVER (ORDER BY policy_start_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_2_prior
    FROM with_details
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id), w_policy_status_policy_id AS (PARTITION BY policy_status, policy_id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        reported_at,
        policy_start_date,
        policy_holder_id,
        claimed_amount_cume_dist,
        next_reported_at,
        claimed_amount_vs_policy_status_policy_id_avg,
        rows_in_policy_status_policy_id,
        claimed_amount_sum_2,
        first_policy_start_date,
        claimed_amount_max_2_prior,
        COUNT(*) OVER (PARTITION BY policy_status) AS rows_in_policy_status,
        LEAD(reported_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY claimed_amount_max_2_prior DESC NULLS FIRST, id) AS next3_reported_at,
        LAG(reported_at, 3) OVER (PARTITION BY policy_status ORDER BY first_policy_start_date DESC NULLS FIRST, id) AS prev3_reported_at,
        ROUND(AVG(claimed_amount_percent_rank) OVER (PARTITION BY policy_status ORDER BY first_policy_start_date DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_percent_rank_avg_7,
        NTILE(10) OVER w_policy_status_by_first_polic AS first_policy_start_date_decile
    FROM ranked
    WINDOW w_policy_status_by_first_polic AS (PARTITION BY policy_status ORDER BY first_policy_start_date DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    reported_at,
    policy_start_date,
    policy_holder_id,
    claimed_amount_cume_dist,
    next_reported_at,
    claimed_amount_vs_policy_status_policy_id_avg,
    rows_in_policy_status_policy_id,
    claimed_amount_sum_2,
    first_policy_start_date,
    claimed_amount_max_2_prior,
    rows_in_policy_status,
    prev3_reported_at,
    first_policy_start_date_decile
FROM windowed
WHERE claimed_amount_vs_policy_status_policy_id_avg <= 10000.0
ORDER BY policy_start_date DESC
