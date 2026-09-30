WITH cleaned AS (
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
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE claim_date < DATE '2024-12-22'
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_date,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name,
        p.product AS policy_product
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE a.team IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        approved_amount,
        status,
        adjuster_level,
        adjuster_name,
        DENSE_RANK() OVER w_policy_id_claim_type_by_appr AS approved_amount_dense_rank,
        NTH_VALUE(claim_date, 2) OVER (ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        SUM(adjuster_level) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_adjuster_level,
        NTILE(3) OVER w_all_by_reported_at AS reported_at_tercile,
        MIN(approved_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_min_approved_amount,
        ROW_NUMBER() OVER (ORDER BY adjuster_level, id) AS adjuster_level_seq,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        SUM(approved_amount) OVER w_status AS status_sum_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id), 0), 4) AS approved_amount_share_of_adjuster_id,
        MAX(adjuster_level) OVER (ORDER BY adjuster_level, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS adjuster_level_max_2_prior
    FROM with_details
    WINDOW w_policy_id_claim_type_by_appr AS (PARTITION BY policy_id, claim_type ORDER BY approved_amount), w_all_by_reported_at AS (ORDER BY reported_at, id), w_status AS (PARTITION BY status)
),
scored AS (
    -- Add calculated columns
    SELECT *, FLOOR(reported_at_tercile / 2) * 2 AS reported_at_tercile_bucket
    FROM windowed
    WHERE adjuster_id_sum_adjuster_level >= 23
)
SELECT
    id,
    approved_amount,
    approved_amount_dense_rank,
    second_claim_date,
    adjuster_id_sum_adjuster_level,
    reported_at_tercile,
    adjuster_id_min_approved_amount,
    rows_in_status,
    status_sum_approved_amount,
    approved_amount_share_of_adjuster_id,
    reported_at_tercile_bucket,
    CASE WHEN status IN ('PAID', 'APPROVED') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
    rows_in_status + adjuster_level_seq AS rows_in_status_plus_adjuster_level_seq
FROM scored
ORDER BY approved_amount DESC
