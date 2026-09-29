WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE policy_id IS NOT NULL
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.level AS adjuster_level,
        p.status AS policy_status
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
latest_per_key AS (
    -- Keep the latest row per claim_type, policy_status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        adjuster_level,
        policy_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, policy_status ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM combined_details_adjuster
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        adjuster_level,
        policy_status,
        CONCAT_WS(': ', status, policy_status) AS status_policy_status_label,
        COALESCE(adjuster_level, 0) AS adjuster_level_filled,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM latest_per_key
)
SELECT
    adjuster_level_filled,
    policy_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows
FROM enriched
GROUP BY ROLLUP(adjuster_level_filled, policy_id)
ORDER BY adjuster_level_filled NULLS LAST, policy_id NULLS LAST
