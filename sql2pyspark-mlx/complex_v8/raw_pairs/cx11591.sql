WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE status = 'PAID'
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, a.adjuster_name, a.level AS adjuster_level
    FROM cleaned s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY status, policy_id ORDER BY claim_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_3,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id), 0), 4) AS claimed_amount_share_of_policy_id,
        FIRST_VALUE(reported_at) OVER (PARTITION BY claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        SUM(claimed_amount) OVER w_policy_id AS policy_id_sum_claimed_amount,
        SUM(adjuster_level) OVER (PARTITION BY claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level,
        claimed_amount - LAG(claimed_amount) OVER w_claim_date_month_by_claim_da AS claimed_amount_change,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date NULLS LAST, id), 3) AS claim_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, policy_id ORDER BY claim_date), 3) AS claim_date_percent_rank,
        MAX(adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS adjuster_level_max_6_prior,
        LEAD(reported_at) OVER (PARTITION BY status, policy_id ORDER BY claim_date, id) AS next_reported_at,
        LAG(claim_date) OVER w_claim_type_by_claimed_amount_3 AS prev_claim_date,
        NTILE(4) OVER (PARTITION BY claim_type ORDER BY claimed_amount ASC, id) AS claimed_amount_quartile
    FROM enriched
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_claim_date_month_by_claim_da AS (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date NULLS LAST, id), w_claim_type_by_claimed_amount_3 AS (PARTITION BY claim_type ORDER BY claimed_amount ASC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        adjuster_name,
        adjuster_level,
        rows_in_policy_id,
        first_reported_at,
        policy_id_sum_claimed_amount,
        running_adjuster_level,
        claimed_amount_change,
        claim_date_cume_dist,
        claim_date_percent_rank,
        adjuster_level_max_6_prior,
        next_reported_at,
        claimed_amount_quartile,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len,
        DATEDIFF(claim_date, prev_claim_date) AS days_prev_claim_date_to_claim_date,
        ROUND(running_adjuster_level / NULLIF(claimed_amount, 0), 3) AS running_adjuster_level_per_claimed_amount
    FROM with_windows
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    adjuster_level,
    policy_id_sum_claimed_amount,
    running_adjuster_level,
    claim_date_cume_dist,
    adjuster_level_max_6_prior,
    next_reported_at,
    days_prev_claim_date_to_claim_date,
    running_adjuster_level_per_claimed_amount,
    DATE_FORMAT(next_reported_at, 'yyyy-MM-dd') AS next_reported_at_label
FROM annotated
ORDER BY policy_id, claim_type
