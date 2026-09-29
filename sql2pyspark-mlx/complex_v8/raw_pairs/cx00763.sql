WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        CASE WHEN status IN ('REJECTED', 'APPROVED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE claimed_amount >= 4554.8
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_id,
        claim_type,
        status_group,
        ROUND(approved_amount * 1.07, 2) AS approved_amount_adj,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM base
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM enriched s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(approved_amount_adj) OVER w_status_group AS status_group_min_approved_amount_adj,
        NTILE(4) OVER (ORDER BY approved_amount_adj DESC, id) AS approved_amount_adj_quartile,
        ROUND(claimed_amount_bucket / NULLIF(SUM(claimed_amount_bucket) OVER (PARTITION BY status_group), 0), 4) AS claimed_amount_bucket_share_of_status_group,
        LAST_VALUE(claim_count) IGNORE NULLS OVER (ORDER BY approved_amount_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_count,
        ROW_NUMBER() OVER (ORDER BY approved_amount_adj DESC, id) AS rn
    FROM with_stats
    WINDOW w_status_group AS (PARTITION BY status_group)
)
SELECT *
FROM (
    SELECT first_claim_date, approved_amount_adj_quartile, claim_type, claimed_amount_bucket_share_of_status_group
    FROM windowed
) src
PIVOT (
    AVG(claimed_amount_bucket_share_of_status_group)
    FOR claim_type IN ('medical', 'theft', 'collision')
)
