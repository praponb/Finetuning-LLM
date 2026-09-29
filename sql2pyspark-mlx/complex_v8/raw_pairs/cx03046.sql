WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        MONTH(end_date) AS end_date_month,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE product IS NOT NULL
      AND premium < 3817.5
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date,
        c.approved_amount AS claim_approved_amount
    FROM filtered s
    JOIN claims c ON c.policy_id = s.id
),
holder_id_stats AS (
    -- Median claim_approved_amount per holder_id
    SELECT holder_id, PERCENTILE_APPROX(claim_approved_amount, 0.5) AS median_claim_approved_amount
    FROM detail_rows
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claim_approved_amount,
        ROUND(s.claim_approved_amount - g.median_claim_approved_amount, 2) AS claim_approved_amount_minus_median
    FROM detail_rows s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.claim_approved_amount >= g.median_claim_approved_amount
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(claim_approved_amount, 0.0) AS claim_approved_amount_filled,
        ABS(claim_approved_amount - 14595.6) AS claim_approved_amount_distance,
        FLOOR(claim_approved_amount / 10000) * 10000 AS claim_approved_amount_bucket
    FROM compared
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        claim_id,
        claim_adjuster_id,
        claim_approved_amount,
        median_claim_approved_amount,
        claim_approved_amount_minus_median,
        ROUND(claim_approved_amount * 0.25, 2) AS claim_approved_amount_adj,
        ROUND(claim_approved_amount * 0.25, 2) AS claim_approved_amount_adj_2,
        FLOOR(claim_approved_amount / 10000) * 10000 AS claim_approved_amount_bucket_2
    FROM scored
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.start_date,
        s.end_date,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_approved_amount,
        s.claim_approved_amount_minus_median,
        s.claim_approved_amount_adj,
        s.claim_approved_amount_adj_2,
        s.claim_approved_amount_bucket_2,
        a.team AS adjuster_team,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM scored_2 s
    JOIN adjusters a ON a.id = s.claim_adjuster_id
    LEFT JOIN claims c ON c.id = s.claim_id
    WHERE a.team = 'Motor'
)
SELECT
    id,
    end_date,
    claim_id,
    claim_approved_amount,
    claim_approved_amount_minus_median,
    claim_approved_amount_adj,
    claim_approved_amount_bucket_2,
    adjuster_team,
    claim_claimed_amount,
    ROUND(claim_approved_amount * 1.5, 2) AS claim_approved_amount_adj_3
FROM enriched_adjuster
WHERE claim_approved_amount_adj_2 < 60000.0
ORDER BY claim_claimed_amount DESC
