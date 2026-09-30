WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE approved_amount > 0.0
),
with_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_type_len,
        s.claim_type_ref,
        p.end_date AS policy_end_date
    FROM recent s
    LEFT JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(approved_amount) OVER (PARTITION BY policy_id ORDER BY policy_end_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_sum_7,
        ROW_NUMBER() OVER w_policy_id_by_policy_end_date_2 AS rn,
        COUNT(*) OVER w_reported_at_to_date_status AS rows_in_reported_at_to_date_status,
        LAG(policy_end_date) OVER (PARTITION BY policy_id ORDER BY policy_end_date ASC, id) AS prev_policy_end_date,
        SUM(approved_amount) OVER (PARTITION BY TO_DATE(reported_at), status) AS reported_at_to_date_status_sum_approved_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY approved_amount, id), 3) AS approved_amount_percent_rank,
        LAST_VALUE(approved_amount) OVER (PARTITION BY policy_id ORDER BY policy_end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY claim_type ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount
    FROM with_details
    WINDOW w_policy_id_by_policy_end_date_2 AS (PARTITION BY policy_id ORDER BY policy_end_date ASC, id), w_reported_at_to_date_status AS (PARTITION BY TO_DATE(reported_at), status)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_type_len,
        claim_type_ref,
        policy_end_date,
        approved_amount_sum_7,
        rows_in_reported_at_to_date_status,
        prev_policy_end_date,
        reported_at_to_date_status_sum_approved_amount,
        approved_amount_percent_rank,
        last_approved_amount,
        first_approved_amount
    FROM ranked
    WHERE rn <= 2
      AND prev_policy_end_date IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claim_type)) AS distinct_status_in_claim_type,
        MAX(first_approved_amount) OVER (PARTITION BY claim_type) AS claim_type_max_first_approved_amount,
        ROUND(PERCENT_RANK() OVER w_claim_type_adjuster_id_by_cl, 3) AS claim_type_len_percent_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY claim_type_len ORDER BY approved_amount_percent_rank DESC, id))) / 60, 1) AS minutes_since_prev_reported_at
    FROM first_rows
    WINDOW w_claim_type_adjuster_id_by_cl AS (PARTITION BY claim_type, adjuster_id ORDER BY claim_type_len)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    status,
    reported_at,
    claim_type_len,
    claim_type_ref,
    policy_end_date,
    approved_amount_sum_7,
    rows_in_reported_at_to_date_status,
    prev_policy_end_date,
    reported_at_to_date_status_sum_approved_amount,
    last_approved_amount,
    claim_type_len_percent_rank,
    minutes_since_prev_reported_at,
    CASE WHEN approved_amount >= 20880.4 THEN 'HIGH' WHEN approved_amount >= 799.5 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
FROM with_windows
