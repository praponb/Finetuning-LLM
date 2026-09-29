WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM claims
    WHERE claimed_amount > 12726.7
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY claim_type_or_unknown, adjuster_id), 2) AS claimed_amount_vs_claim_type_or_unknown_adjuster,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS approved_amount_max_4,
        NTILE(10) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS approved_amount_decile
    FROM scoped_claims
    WINDOW w_claim_type_or_unknown_adjust AS (PARTITION BY claim_type_or_unknown, adjuster_id)
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claim_type_or_unknown,
        s.status_ref,
        s.first_known_approved_amount,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        a.level AS adjuster_level
    FROM metrics s
    JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ADD_MONTHS(policy_end_date, 1) AS policy_end_date_plus_1m,
        SUBSTRING(TRIM(claim_type_or_unknown), 1, 4) AS claim_type_or_unknown_prefix
    FROM joined_policy
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(adjuster_level, 0) AS adjuster_level_filled,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at,
        INITCAP(TRIM(claim_type_or_unknown)) AS claim_type_or_unknown_clean
    FROM derived
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.status_ref,
        s.first_known_approved_amount,
        s.policy_start_date,
        s.policy_holder_id,
        s.policy_end_date,
        s.adjuster_level,
        s.claim_type_or_unknown_prefix,
        s.adjuster_level_filled,
        s.days_since_reported_at,
        s.claim_type_or_unknown_clean,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.policy_id AS claim_policy_id
    FROM derived_2 s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.approved_amount >= 0.0
),
unique_rows AS (
    -- Keep the earliest row per claim_policy_id, policy_holder_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        status_ref,
        first_known_approved_amount,
        policy_start_date,
        policy_holder_id,
        policy_end_date,
        adjuster_level,
        claim_type_or_unknown_prefix,
        adjuster_level_filled,
        days_since_reported_at,
        claim_type_or_unknown_clean,
        claim_id,
        claim_reported_at,
        claim_approved_amount,
        claim_claimed_amount,
        claim_policy_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_policy_id, policy_holder_id ORDER BY claim_claimed_amount, id, claim_id) AS recency_rank
        FROM exploded_detail_claims
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_level,
    adjuster_level_filled,
    GROUPING(adjuster_level) AS is_total_adjuster_level,
    PERCENTILE_APPROX(days_since_reported_at, 0.5) AS median_days_since_reported_at
FROM unique_rows
GROUP BY GROUPING SETS ((adjuster_level, adjuster_level_filled), (adjuster_level), (adjuster_level_filled))
ORDER BY adjuster_level NULLS LAST, adjuster_level_filled NULLS LAST
