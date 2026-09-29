WITH recent_claims AS (
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
        CASE WHEN status IN ('OPEN', 'APPROVED') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group,
        YEAR(reported_at) AS reported_at_year,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount < 17759.3
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        status_group,
        least_claimed_amount_approved_amount,
        ROUND(AVG(reported_at_year) OVER w_reported_at_year_claim_type, 2) AS reported_at_year_claim_type_avg_reported_at_year,
        MAX(reported_at_year) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS reported_at_year_max_2_prior,
        FIRST_VALUE(reported_at) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS approved_amount_change
    FROM with_activity
    WINDOW w_reported_at_year_claim_type AS (PARTITION BY YEAR(reported_at), claim_type)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY least_claimed_amount_approved_amount, id) AS least_claimed_amount_approved_amount_dense_rank,
        COUNT(*) OVER w_policy_id_claim_type AS rows_in_policy_id_claim_type,
        MAX(approved_amount_change) OVER (PARTITION BY adjuster_id ORDER BY least_claimed_amount_approved_amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_change_max_6_prior,
        ROUND(AVG(reported_at_year_max_2_prior) OVER (PARTITION BY adjuster_id ORDER BY least_claimed_amount_approved_amount, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS reported_at_year_max_2_prior_avg_2,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY least_claimed_amount_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(reported_at_year_max_2_prior - AVG(reported_at_year_max_2_prior) OVER w_policy_id_claim_type, 2) AS reported_at_year_max_2_prior_vs_policy_id_claim_
    FROM sequenced
    WINDOW w_policy_id_claim_type AS (PARTITION BY policy_id, claim_type)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE reported_at_year_max_2_prior_vs_policy_id_claim_ < (SELECT MAX(reported_at_year_max_2_prior_vs_policy_id_claim_) FROM metrics)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    status_group,
    least_claimed_amount_approved_amount,
    reported_at_year_claim_type_avg_reported_at_year,
    reported_at_year_max_2_prior,
    first_reported_at,
    approved_amount_change,
    least_claimed_amount_approved_amount_dense_rank,
    rows_in_policy_id_claim_type,
    approved_amount_change_max_6_prior,
    reported_at_year_max_2_prior_avg_2,
    first_known_reported_at,
    reported_at_year_max_2_prior_vs_policy_id_claim_,
    DATE_TRUNC('YEAR', first_known_reported_at) AS first_known_reported_at_year
FROM screened
ORDER BY first_reported_at, least_claimed_amount_approved_amount DESC
