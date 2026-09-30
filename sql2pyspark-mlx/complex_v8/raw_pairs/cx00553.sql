WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE approved_amount >= 2349.3
      AND claim_date BETWEEN DATE '2024-06-20' AND DATE '2025-04-11'
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id
    FROM prepared s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium <= 4417.0
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(claimed_amount_filled, 3) OVER (PARTITION BY adjuster_id ORDER BY approved_amount ASC, id) AS next3_claimed_amount_filled,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY approved_amount ASC, id) AS seq_no,
        ROUND(AVG(claimed_amount_filled) OVER (PARTITION BY MONTH(policy_end_date) ORDER BY approved_amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_filled_avg_6_prior,
        LAG(reported_at) OVER w_policy_id_reported_at_year_b AS prev_reported_at,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, YEAR(reported_at) ORDER BY policy_end_date DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, YEAR(reported_at) ORDER BY policy_end_date DESC, id), 3) AS policy_end_date_percent_rank,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_holder_id, claim_type), 0), 4) AS claimed_amount_share_of_policy_holder_id_claim_t,
        COUNT(*) OVER w_reported_at_month_claim_type AS rows_in_reported_at_month_claim_type,
        NTILE(10) OVER (PARTITION BY adjuster_id ORDER BY approved_amount ASC, id) AS approved_amount_decile,
        LAST_VALUE(reported_at) OVER (PARTITION BY policy_id, YEAR(reported_at) ORDER BY policy_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        MIN(policy_premium) OVER (PARTITION BY policy_holder_id, claim_type) AS policy_holder_id_claim_type_min_policy_premium,
        NTILE(3) OVER (PARTITION BY adjuster_id, YEAR(reported_at) ORDER BY policy_end_date DESC, id) AS policy_end_date_tercile,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id, YEAR(reported_at) ORDER BY policy_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY approved_amount ASC) AS approved_amount_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, YEAR(reported_at) ORDER BY policy_end_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4
    FROM combined_details_policy
    WINDOW w_policy_id_reported_at_year_b AS (PARTITION BY policy_id, YEAR(reported_at) ORDER BY policy_premium DESC, id), w_reported_at_month_claim_type AS (PARTITION BY MONTH(reported_at), claim_type)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    reported_at,
    claimed_amount_filled,
    policy_premium,
    policy_end_date,
    policy_holder_id,
    seq_no,
    claimed_amount_filled_avg_6_prior,
    prev_reported_at,
    rn,
    policy_end_date_percent_rank,
    claimed_amount_share_of_policy_holder_id_claim_t,
    rows_in_reported_at_month_claim_type,
    approved_amount_decile,
    last_reported_at,
    policy_holder_id_claim_type_min_policy_premium,
    first_known_approved_amount,
    approved_amount_rank,
    claimed_amount_avg_4
FROM metrics
ORDER BY seq_no DESC NULLS FIRST, last_reported_at DESC
