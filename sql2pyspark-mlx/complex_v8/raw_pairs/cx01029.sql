WITH valid_claims AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE approved_amount < 20843.1
      AND claim_date IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        CASE WHEN approved_amount >= 17759.3 THEN 'HIGH' WHEN approved_amount >= 7204.1 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM valid_claims
    WHERE claim_type = 'flood'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(approved_amount) AS max_claim_approved_amount,
        MAX(approved_amount) AS max_claim_approved_amount_2
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_approved_amount,
        c.max_claim_approved_amount_2
    FROM tagged s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
latest_per_key AS (
    -- Keep the latest row per status
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        approved_amount_filled,
        approved_amount_tier,
        max_claim_approved_amount,
        max_claim_approved_amount_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY claimed_amount DESC, id DESC) AS recency_rank
        FROM with_counts
    ) ranked
    WHERE recency_rank = 1
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.approved_amount_filled,
        s.approved_amount_tier,
        s.max_claim_approved_amount_2,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM latest_per_key s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team = 'Health'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        s.approved_amount_filled,
        s.adjuster_team,
        s.adjuster_name,
        a.team AS adjuster_team_2,
        a.adjuster_name AS adjuster_adjuster_name,
        a.level AS adjuster_level
    FROM with_parent_adjuster s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_parent AS (
    -- Join adjusters
    SELECT
        s.id,
        s.adjuster_id,
        s.status,
        s.adjuster_team,
        s.adjuster_name,
        s.adjuster_team_2,
        s.adjuster_level,
        a.level AS adjuster_level_2,
        a.team AS adjuster_team_3,
        a.adjuster_name AS adjuster_adjuster_name_2
    FROM enriched s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    adjuster_id,
    adjuster_name,
    adjuster_team_2,
    adjuster_level,
    adjuster_team_3,
    CASE WHEN adjuster_level_2 >= 3 THEN 'A' WHEN adjuster_level_2 >= 1 THEN 'B' ELSE 'C' END AS adjuster_level_2_tier
FROM with_parent
ORDER BY id NULLS LAST, adjuster_name DESC
