WITH valid_claims AS (
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
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-19 14:00:00'
      AND claimed_amount BETWEEN 7204.1 AND 41760.8
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY status ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY claim_type, YEAR(claim_date) ORDER BY claimed_amount, id)) AS days_since_prev_claim_date
    FROM valid_claims
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.status,
        s.second_claim_date,
        s.days_since_prev_claim_date,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_type
    FROM windowed s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(days_since_prev_claim_date - AVG(days_since_prev_claim_date) OVER w_claim_date_year_claim_id, 2) AS days_since_prev_claim_date_vs_claim_date_year_cl,
        RANK() OVER (ORDER BY second_claim_date ASC) AS second_claim_date_rank,
        SUM(days_since_prev_claim_date) OVER (PARTITION BY claim_status, claim_type ORDER BY claim_date DESC, id, claim_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_prev_claim_date_sum_3_prior,
        COUNT(*) OVER (PARTITION BY YEAR(claim_date), claim_id) AS rows_in_claim_date_year_claim_id,
        LEAD(second_claim_date, 2) OVER (ORDER BY second_claim_date ASC, id, claim_id) AS next2_second_claim_date,
        NTH_VALUE(claim_date, 2) OVER (ORDER BY second_claim_date ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date_2
    FROM line_level_claims
    WINDOW w_claim_date_year_claim_id AS (PARTITION BY YEAR(claim_date), claim_id)
),
scored AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM with_windows
    WHERE claim_type IN ('collision', 'delay')
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM scored
    WHERE second_claim_date_rank = 1
      AND claim_date < DATE '2025-01-10'
),
with_details_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.status,
        s.second_claim_date,
        s.claim_id,
        s.claim_status,
        s.claim_type,
        s.days_since_prev_claim_date_vs_claim_date_year_cl,
        s.second_claim_date_rank,
        s.days_since_prev_claim_date_sum_3_prior,
        s.next2_second_claim_date,
        s.second_claim_date_2,
        s.days_since_claim_date,
        c.status AS claim_status_2,
        c.claim_date AS claim_claim_date,
        c.adjuster_id AS claim_adjuster_id,
        p.status AS policy_status,
        p.end_date AS policy_end_date
    FROM top_per_group s
    JOIN claims c ON c.id = s.claim_id
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_id,
    claim_type,
    days_since_prev_claim_date_vs_claim_date_year_cl,
    second_claim_date_rank,
    second_claim_date_2,
    days_since_claim_date,
    claim_status_2,
    claim_claim_date,
    claim_adjuster_id,
    policy_status,
    policy_end_date
FROM with_details_claim
WHERE claim_type = 'flood'
ORDER BY second_claim_date_2 DESC
