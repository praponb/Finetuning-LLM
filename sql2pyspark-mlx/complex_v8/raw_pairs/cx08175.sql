WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE status = 'PAID'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)), 0), 4) AS approved_amount_share_of_reported_at_to_date,
        ROUND(AVG(claimed_amount) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_3_prior,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_reported_at_to_date, 2) AS claimed_amount_vs_reported_at_to_date_avg,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY policy_id, adjuster_id ORDER BY greatest_claimed_amount_approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY greatest_claimed_amount_approved_amount ASC, id) AS claimed_amount_change,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        MIN(approved_amount) OVER (PARTITION BY MONTH(reported_at), policy_id) AS reported_at_month_policy_id_min_approved_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY MONTH(reported_at), policy_id), 0), 4) AS claimed_amount_share_of_reported_at_month_policy,
        NTILE(5) OVER (ORDER BY claimed_amount DESC, id) AS claimed_amount_quintile
    FROM cleaned_claims
    WINDOW w_reported_at_to_date AS (PARTITION BY TO_DATE(reported_at))
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(reported_at) OVER (PARTITION BY MONTH(claim_date), DATE_TRUNC('MONTH', reported_at) ORDER BY claimed_amount_share_of_reported_at_month_policy DESC, id) AS next_reported_at,
        ROUND(first_known_claimed_amount / NULLIF(SUM(first_known_claimed_amount) OVER (PARTITION BY adjuster_id), 0), 4) AS first_known_claimed_amount_share_of_adjuster_id,
        MAX(claimed_amount_avg_3_prior) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY approved_amount_share_of_reported_at_to_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_avg_3_prior_max_3_prior,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY approved_amount_share_of_reported_at_to_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount
    FROM windowed
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    approved_amount_share_of_reported_at_to_date,
    claimed_amount_vs_reported_at_to_date_avg,
    second_approved_amount,
    claimed_amount_change,
    first_known_claimed_amount,
    reported_at_month_policy_id_min_approved_amount,
    claimed_amount_share_of_reported_at_month_policy,
    next_reported_at,
    first_known_claimed_amount_share_of_adjuster_id,
    claimed_amount_avg_3_prior_max_3_prior,
    first_claimed_amount,
    LEAST(claimed_amount_change, second_approved_amount) AS least_claimed_amount_change_second_approved_amou
FROM sequenced
WHERE claimed_amount_avg_3_prior_max_3_prior BETWEEN 7595.0 AND 112525.0
ORDER BY first_known_claimed_amount DESC
