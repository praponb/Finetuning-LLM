WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_type IN ('flood', 'theft')
      AND approved_amount > 2272.7
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        a.adjuster_name,
        p.end_date AS policy_end_date
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
    JOIN policies p ON p.id = s.policy_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
        )
      AND s.adjuster_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        adjuster_name,
        policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY policy_end_date DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
)
SELECT
    policy_id,
    adjuster_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt
FROM unique_rows
GROUP BY ROLLUP(policy_id, adjuster_id)
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST
