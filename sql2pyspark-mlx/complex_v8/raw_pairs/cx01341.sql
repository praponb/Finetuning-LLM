WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE (MONTH(reported_at) IN (1, 2) OR status IS NOT NULL)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY claim_type, adjuster_id), 2) AS claim_type_adjuster_id_avg_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type, adjuster_id), 2) AS approved_amount_vs_claim_type_adjuster_id_avg,
        NTILE(3) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date, id) AS claim_date_tercile,
        RANK() OVER (PARTITION BY MONTH(claim_date), claim_type ORDER BY claim_date DESC, id) AS claim_date_rank
    FROM filtered
),
summary AS (
    -- Group-level totals
    SELECT
        claim_date_tercile,
        MIN(approved_amount) AS min_approved_amount,
        COUNT(*) AS n_rows,
        SUM(claim_type_adjuster_id_avg_approved_amount) AS total_claim_type_adjuster_id_avg_approved_amount,
        MIN(greatest_approved_amount_claimed_amount) AS min_greatest_approved_amount_claimed_amount
    FROM windowed
    GROUP BY claim_date_tercile
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        min_approved_amount + claim_date_tercile AS min_approved_amount_plus_claim_date_tercile,
        GREATEST(claim_date_tercile, min_approved_amount) AS greatest_claim_date_tercile_min_approved_amount,
        min_approved_amount - claim_date_tercile AS min_approved_amount_minus_claim_date_tercile
    FROM summary
),
tagged AS (
    -- Add calculated columns
    SELECT
        claim_date_tercile,
        min_approved_amount,
        total_claim_type_adjuster_id_avg_approved_amount,
        min_greatest_approved_amount_claimed_amount,
        min_approved_amount_plus_claim_date_tercile,
        min_approved_amount_minus_claim_date_tercile,
        ABS(claim_date_tercile - 6) AS claim_date_tercile_distance
    FROM enriched
    WHERE min_approved_amount_minus_claim_date_tercile >= 15003.0
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(min_approved_amount, 0.0) AS min_approved_amount_filled
    FROM tagged
)
SELECT
    claim_date_tercile,
    min_approved_amount,
    total_claim_type_adjuster_id_avg_approved_amount,
    claim_date_tercile_distance,
    min_approved_amount_filled,
    FLOOR(min_approved_amount / 10000) * 10000 AS min_approved_amount_bucket,
    claim_date_tercile - min_approved_amount AS claim_date_tercile_minus_min_approved_amount
FROM labeled
ORDER BY claim_date_tercile_distance DESC, claim_date_tercile
LIMIT 5
