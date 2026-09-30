WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
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
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE approved_amount <= 14668.0
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        CASE WHEN claimed_amount >= 39666.5 THEN 'A' WHEN claimed_amount >= 17759.3 THEN 'B' ELSE 'C' END AS claimed_amount_tier
    FROM valid_claims
),
deduped AS (
    -- Keep the latest row per claimed_amount_tier
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        claimed_amount_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claimed_amount_tier ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM tagged
    ) ranked
    WHERE recency_rank = 1
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount_tier,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount
    FROM deduped s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.approved_amount >= 2272.7
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_id,
        s.claim_claim_type,
        s.claim_approved_amount,
        c.id AS claim_id_2,
        c.claim_date,
        c.claim_type AS claim_claim_type_2,
        c.claimed_amount AS claim_claimed_amount
    FROM exploded_detail s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claim_id,
        claim_claim_type,
        claim_approved_amount,
        claim_id_2,
        claim_date,
        claim_claim_type_2,
        claim_claimed_amount,
        LAG(claim_claimed_amount, 3, 0) OVER (PARTITION BY adjuster_id ORDER BY claim_approved_amount NULLS LAST, id, claim_id, claim_id_2) AS prev3_claim_claimed_amount,
        claim_approved_amount - LAG(claim_approved_amount) OVER w_adjuster_id_by_claim_approve AS claim_approved_amount_change,
        MAX(claim_approved_amount) OVER (PARTITION BY adjuster_id ORDER BY claim_approved_amount NULLS LAST, id, claim_id, claim_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claim_approved_amount_so_far
    FROM exploded_detail_claims
    WINDOW w_adjuster_id_by_claim_approve AS (PARTITION BY adjuster_id ORDER BY claim_approved_amount NULLS LAST, id, claim_id, claim_id_2)
)
SELECT *
FROM (
    SELECT claim_id, claim_id_2, claim_claim_type, claim_claimed_amount
    FROM sequenced
) src
PIVOT (
    COUNT(claim_claimed_amount)
    FOR claim_claim_type IN ('delay', 'fire', 'theft')
)
ORDER BY claim_id, claim_id_2
