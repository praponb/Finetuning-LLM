WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        ABS(premium - 2289.0) AS premium_distance,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE end_date BETWEEN DATE '2025-09-27' AND DATE '2027-03-20'
),
enriched AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM scoped
    WHERE status_ref IS NOT NULL
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status_ref,
        s.premium_bucket,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at,
        c.claim_date
    FROM enriched s
    LEFT JOIN claims c ON c.policy_id = s.id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY claim_date, id, claim_id) AS rn,
        SUM(claim_claimed_amount) OVER (PARTITION BY claim_id) AS claim_id_sum_claim_claimed_amount,
        ROUND(claim_claimed_amount - AVG(claim_claimed_amount) OVER (PARTITION BY claim_id), 2) AS claim_claimed_amount_vs_claim_id_avg,
        COUNT(*) OVER (PARTITION BY claim_id) AS rows_in_claim_id,
        NTILE(10) OVER (ORDER BY claim_date, id, claim_id) AS claim_date_decile
    FROM with_children
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    rn,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(claim_date_decile), 2) AS stddev_claim_date_decile,
    SUM(claim_claimed_amount) AS total_claim_claimed_amount,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
FROM metrics
GROUP BY GROUPING SETS ((holder_id, rn), (holder_id), (rn))
