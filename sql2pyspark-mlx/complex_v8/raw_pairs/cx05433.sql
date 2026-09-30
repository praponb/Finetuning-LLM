WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(claimed_amount, 1, 0) OVER w_all_by_claim_date AS prev_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id, policy_id)) AS distinct_claim_type_in_adjuster_id_policy_id,
        ROW_NUMBER() OVER (ORDER BY claim_date, id) AS rn,
        FIRST_VALUE(approved_amount_filled) IGNORE NULLS OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount_filled,
        LEAD(reported_at) OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount DESC, id) AS next_reported_at,
        NTILE(10) OVER w_all_by_claim_date AS claim_date_decile,
        LEAD(claimed_amount) OVER (ORDER BY claim_date, id) AS next_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER w_claim_date_month_reported_at AS claimed_amount_change,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount_filled NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_2_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_claim_date, 3) AS claim_date_percent_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        FIRST_VALUE(approved_amount_filled) OVER (PARTITION BY adjuster_id ORDER BY approved_amount_filled NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount_filled
    FROM base
    WINDOW w_all_by_claim_date AS (ORDER BY claim_date, id), w_claim_date_month_reported_at AS (PARTITION BY MONTH(claim_date), MONTH(reported_at) ORDER BY claim_date, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    claimed_amount_per_approved_amount,
    approved_amount_filled,
    prev_claimed_amount,
    distinct_claim_type_in_adjuster_id_policy_id,
    first_known_approved_amount_filled,
    claim_date_decile,
    claimed_amount_max_2_prior,
    claim_date_percent_rank,
    approved_amount_vs_claim_type_avg,
    first_approved_amount_filled
FROM metrics
WHERE prev_claimed_amount > 18478.3
ORDER BY approved_amount_vs_claim_type_avg NULLS LAST
