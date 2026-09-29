WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LOWER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claim_date < DATE '2025-04-25'
      AND claimed_amount <= 41760.8
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claim_type_clean,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_clean,
        adjuster_name,
        adjuster_team,
        LEAD(approved_amount) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY approved_amount, id) AS next_approved_amount,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_id, TO_DATE(reported_at)), 2) AS adjuster_level_vs_policy_id_reported_at_to_date_,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY TO_DATE(reported_at) ORDER BY claimed_amount DESC, id) AS adjuster_level_change,
        LEAD(claimed_amount) OVER (PARTITION BY adjuster_level, adjuster_team ORDER BY reported_at DESC, id) AS next_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_id_reported_at_to_dat, 2) AS claimed_amount_vs_policy_id_reported_at_to_date_,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_level), 2) AS claimed_amount_vs_adjuster_level_avg,
        FIRST_VALUE(adjuster_level) OVER (PARTITION BY adjuster_team, DATE_TRUNC('MONTH', reported_at) ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_adjuster_level,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_adjuster_level, 2) AS adjuster_level_vs_adjuster_level_avg,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount
    FROM joined_adjuster
    WINDOW w_policy_id_reported_at_to_dat AS (PARTITION BY policy_id, TO_DATE(reported_at)), w_adjuster_level AS (PARTITION BY adjuster_level)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len,
        CONCAT(claim_type_clean, '#', CAST(policy_id AS STRING)) AS claim_type_clean_ref
    FROM ranked
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_clean,
        adjuster_name,
        adjuster_team,
        adjuster_level_vs_policy_id_reported_at_to_date_,
        distinct_policy_id_in_adjuster_id,
        adjuster_level_change,
        next_claimed_amount,
        claimed_amount_vs_policy_id_reported_at_to_date_,
        claimed_amount_vs_adjuster_level_avg,
        adjuster_level_vs_adjuster_level_avg,
        claimed_amount_avg_4,
        running_approved_amount,
        adjuster_name_len,
        claim_type_clean_ref,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY distinct_policy_id_in_adjuster_id), 0), 4) AS approved_amount_share_of_distinct_policy_id_in_a,
        MAX(next_claimed_amount) OVER (PARTITION BY distinct_policy_id_in_adjuster_id) AS distinct_policy_id_in_adjuster_id_max_next_claim,
        COUNT(*) OVER (PARTITION BY distinct_policy_id_in_adjuster_id) AS rows_in_distinct_policy_id_in_adjuster_id
    FROM labeled
)
SELECT DISTINCT
    claim_type_clean_ref,
    claimed_amount_vs_adjuster_level_avg,
    running_approved_amount
FROM sequenced
