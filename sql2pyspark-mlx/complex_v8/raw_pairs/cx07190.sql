WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id >= 27
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        s.claimed_amount_plus_approved_amount,
        p.end_date AS policy_end_date
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        claimed_amount_plus_approved_amount,
        policy_end_date,
        ROW_NUMBER() OVER (PARTITION BY YEAR(claim_date) ORDER BY approved_amount, id) AS seq_no,
        SUM(approved_amount) OVER (ORDER BY claimed_amount_plus_approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_2_prior,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY claimed_amount_plus_approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount_plus_approved_amount, id), 3) AS claimed_amount_plus_approved_amount_percent_rank,
        LAG(policy_end_date, 2) OVER (ORDER BY claimed_amount_plus_approved_amount, id) AS prev2_policy_end_date,
        RANK() OVER (ORDER BY claimed_amount_plus_approved_amount DESC, id) AS claimed_amount_plus_approved_amount_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_status, 0), 4) AS approved_amount_share_of_status,
        COUNT(*) OVER (ORDER BY claimed_amount_plus_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date))) AS distinct_status_in_claim_date_month,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank,
        ROW_NUMBER() OVER w_all_by_approved_amount AS seq_no_2,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY YEAR(claim_date) ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        LEAD(approved_amount) OVER w_policy_id_by_policy_end_date AS next_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY policy_id ORDER BY policy_end_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        COUNT(*) OVER w_claim_date_month AS rows_in_claim_date_month
    FROM enriched
    WINDOW w_status AS (PARTITION BY status), w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_policy_id_by_policy_end_date AS (PARTITION BY policy_id ORDER BY policy_end_date NULLS LAST, id), w_claim_date_month AS (PARTITION BY DATE_TRUNC('MONTH', claim_date))
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', last_known_claim_date) AS days_since_last_known_claim_date
    FROM ranked
)
-- Final projection
SELECT
    id,
    policy_id,
    status,
    claimed_amount_plus_approved_amount,
    seq_no,
    approved_amount_sum_2_prior,
    first_known_approved_amount,
    prev2_policy_end_date,
    claimed_amount_plus_approved_amount_rank,
    approved_amount_share_of_status,
    running_count,
    distinct_status_in_claim_date_month,
    approved_amount_percent_rank,
    seq_no_2,
    last_known_claim_date,
    running_approved_amount,
    rows_in_claim_date_month,
    days_since_last_known_claim_date
FROM derived
ORDER BY claimed_amount_plus_approved_amount_rank DESC, approved_amount_share_of_status
