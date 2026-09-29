WITH filtered AS (
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
        reported_at,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        CASE WHEN claim_type IN ('fire', 'theft') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claimed_amount <= 42838.9
      AND adjuster_id IS NOT NULL
),
with_details AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        p.end_date AS policy_end_date
    FROM filtered s
    LEFT JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        approved_amount_adj,
        claim_type_group,
        policy_holder_id,
        policy_product,
        policy_end_date,
        ROUND(approved_amount_adj / NULLIF(SUM(approved_amount_adj) OVER (PARTITION BY claim_type, YEAR(claim_date)), 0), 4) AS approved_amount_adj_share_of_claim_type_claim_da,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_claim_type_claim_date_year, 0), 4) AS claimed_amount_share_of_claim_type_claim_date_ye,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY claim_date) AS claim_date_dense_rank,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        NTILE(10) OVER (ORDER BY approved_amount DESC NULLS FIRST, id) AS approved_amount_decile,
        COUNT(*) OVER (PARTITION BY status, claim_type_group) AS rows_in_status_claim_type_group,
        NTILE(3) OVER (ORDER BY approved_amount DESC NULLS FIRST, id) AS approved_amount_tercile,
        SUM(approved_amount) OVER (PARTITION BY status, claim_type_group) AS status_claim_type_group_sum_approved_amount,
        LEAD(approved_amount, 3) OVER w_reported_at_month_by_approve AS next3_approved_amount,
        ROUND(PERCENT_RANK() OVER w_policy_id_claim_type_by_clai, 3) AS claim_date_percent_rank,
        COUNT(*) OVER (PARTITION BY claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        LEAD(claim_date) OVER (ORDER BY approved_amount DESC NULLS FIRST, id) AS next_claim_date
    FROM with_details
    WINDOW w_claim_type_claim_date_year AS (PARTITION BY claim_type, YEAR(claim_date)), w_reported_at_month_by_approve AS (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY approved_amount, id), w_policy_id_claim_type_by_clai AS (PARTITION BY policy_id, claim_type ORDER BY claim_date)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE claim_date_dense_rank = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        approved_amount_adj,
        claim_type_group,
        policy_product,
        policy_end_date,
        approved_amount_adj_share_of_claim_type_claim_da,
        claimed_amount_share_of_claim_type_claim_date_ye,
        claim_date_dense_rank,
        last_claimed_amount,
        approved_amount_decile,
        rows_in_status_claim_type_group,
        approved_amount_tercile,
        status_claim_type_group_sum_approved_amount,
        claim_date_percent_rank,
        running_count,
        second_approved_amount,
        next_claim_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_holder_id, status), 2) AS claimed_amount_vs_policy_holder_id_status_avg,
        FIRST_VALUE(running_count) OVER (ORDER BY approved_amount_adj_share_of_claim_type_claim_da ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_running_count,
        COUNT(*) OVER (PARTITION BY approved_amount_decile, policy_product) AS rows_in_approved_amount_decile_policy_product,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY claim_type ORDER BY policy_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount
    FROM leaders
)
SELECT
    id,
    policy_id,
    claim_date,
    approved_amount,
    status,
    policy_product,
    policy_end_date,
    approved_amount_adj_share_of_claim_type_claim_da,
    claimed_amount_share_of_claim_type_claim_date_ye,
    claim_date_dense_rank,
    approved_amount_decile,
    approved_amount_tercile,
    status_claim_type_group_sum_approved_amount,
    claim_date_percent_rank,
    second_approved_amount,
    next_claim_date,
    claimed_amount_vs_policy_holder_id_status_avg,
    first_running_count,
    rows_in_approved_amount_decile_policy_product,
    second_claimed_amount,
    CONCAT_WS(' / ', claim_type, claim_type_group) AS claim_type_claim_type_group_label,
    CASE WHEN approved_amount >= 17759.3 THEN 'HIGH' WHEN approved_amount >= 4868.7 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
FROM windowed
WHERE claimed_amount_vs_policy_holder_id_status_avg <= 20000.0
