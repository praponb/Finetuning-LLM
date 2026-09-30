WITH valid AS (
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
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        claim_type,
        approved_amount,
        claimed_amount_filled,
        ROUND(AVG(claimed_amount_filled) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_claimed_amount_filled,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, YEAR(claim_date_year) ORDER BY reported_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        MIN(approved_amount) OVER (PARTITION BY policy_id, YEAR(claim_date_year) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type), 0), 4) AS approved_amount_share_of_claim_type
    FROM valid
    WINDOW w_claim_type AS (PARTITION BY claim_type)
),
deduped AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        claim_date,
        claim_type,
        approved_amount,
        claimed_amount_filled,
        claim_type_avg_claimed_amount_filled,
        claimed_amount_max_1_prior,
        rows_in_claim_type,
        min_approved_amount_so_far,
        approved_amount_share_of_claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY approved_amount_share_of_claim_type DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_type,
    claimed_amount_filled,
    claim_type_avg_claimed_amount_filled,
    rows_in_claim_type,
    approved_amount_share_of_claim_type
FROM deduped
ORDER BY rows_in_claim_type
