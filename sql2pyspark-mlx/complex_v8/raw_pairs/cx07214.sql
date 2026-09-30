WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status
    FROM policies
    WHERE (end_date >= DATE '2025-12-05' OR start_date < DATE '2025-03-21')
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.claim_date,
        c.status AS claim_status
    FROM cleaned s
    JOIN claims c ON c.policy_id = s.id
),
unique_rows AS (
    -- Keep the earliest row per holder_id, claim_status
    SELECT
        id,
        holder_id,
        premium,
        claim_id,
        claim_approved_amount,
        claim_reported_at,
        claim_date,
        claim_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, claim_status ORDER BY claim_approved_amount, id, claim_id) AS recency_rank
        FROM detail_rows
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    claim_status,
    claim_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt,
    MIN(premium) AS min_premium,
    PERCENTILE_APPROX(claim_approved_amount, 0.5) AS median_claim_approved_amount
FROM unique_rows
GROUP BY GROUPING SETS ((claim_status, claim_id), (claim_status), (claim_id), ())
