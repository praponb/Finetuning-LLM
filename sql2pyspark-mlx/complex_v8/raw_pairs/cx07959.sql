WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE approved_amount >= 0.0
      AND adjuster_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status_prefix,
        claim_type_len,
        ROUND(claim_type_len - AVG(claim_type_len) OVER w_status, 2) AS claim_type_len_vs_status_avg,
        SUM(claim_type_len) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claim_type_len,
        ROW_NUMBER() OVER (ORDER BY claim_date DESC, id) AS seq_no,
        ROUND(claim_type_len / NULLIF(SUM(claim_type_len) OVER w_status, 0), 4) AS claim_type_len_share_of_status
    FROM recent
    WINDOW w_status AS (PARTITION BY status)
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status_prefix,
        claim_type_len_vs_status_avg,
        running_claim_type_len,
        seq_no,
        claim_type_len_share_of_status,
        ROUND(running_claim_type_len / NULLIF(SUM(running_claim_type_len) OVER (PARTITION BY claim_type_len, seq_no), 0), 4) AS running_claim_type_len_share_of_claim_type_len_s,
        ROUND(AVG(running_claim_type_len) OVER (PARTITION BY claim_type_len, seq_no), 2) AS claim_type_len_seq_no_avg_running_claim_type_len,
        ROUND(AVG(claim_type_len_vs_status_avg) OVER (PARTITION BY claim_type_len, seq_no), 2) AS claim_type_len_seq_no_avg_claim_type_len_vs_stat,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type_len, seq_no)) AS distinct_policy_id_in_claim_type_len_seq_no,
        MIN(claim_type_len) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_type_len_vs_status_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claim_type_len_so_far
    FROM ranked
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        status_prefix,
        claim_type_len_vs_status_avg,
        running_claim_type_len,
        running_claim_type_len_share_of_claim_type_len_s,
        distinct_policy_id_in_claim_type_len_seq_no,
        min_claim_type_len_so_far,
        CONCAT(status_prefix, '#', CAST(seq_no AS STRING)) AS status_prefix_ref
    FROM ranked_2
    WHERE claim_type_len_seq_no_avg_claim_type_len_vs_stat < 2.8
)
SELECT
    id,
    policy_id,
    adjuster_id,
    status_prefix,
    claim_type_len_vs_status_avg,
    running_claim_type_len_share_of_claim_type_len_s,
    distinct_policy_id_in_claim_type_len_seq_no,
    min_claim_type_len_so_far,
    status_prefix_ref,
    FLOOR(running_claim_type_len / 25) * 25 AS running_claim_type_len_bucket,
    CASE WHEN running_claim_type_len >= 112 THEN 'large' WHEN running_claim_type_len >= 43 THEN 'medium' ELSE 'small' END AS running_claim_type_len_tier
FROM scored
ORDER BY claim_type_len_vs_status_avg DESC, running_claim_type_len_tier
