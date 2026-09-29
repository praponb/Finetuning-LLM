WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CASE WHEN approved_amount >= 19337.6 THEN 'HIGH' WHEN approved_amount >= 1273.7 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier,
        YEAR(claim_date) AS claim_date_year
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-03-14'
      AND claim_type = 'fire'
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.approved_amount_tier,
        s.claim_date_year,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM valid_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.reported_at >= TIMESTAMP '2025-01-31 14:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        approved_amount_tier,
        claim_id,
        claim_claim_date,
        claim_approved_amount,
        SUM(claim_date_year) OVER (ORDER BY claim_approved_amount DESC NULLS FIRST, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claim_date_year,
        NTILE(10) OVER (ORDER BY claim_approved_amount DESC NULLS FIRST, id, claim_id) AS claim_approved_amount_decile
    FROM detail_rows_claims
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS avg_claim_claimed_amount,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at
    FROM windowed s
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(first_claim_reported_at) AS first_claim_reported_at_year,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        MAX(avg_claim_claimed_amount) AS max_avg_claim_claimed_amount,
        PERCENTILE_APPROX(running_claim_date_year, 0.5) AS median_running_claim_date_year,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        DATEDIFF(MAX(claim_claim_date), MIN(claim_claim_date)) AS claim_claim_date_span_days
    FROM summarized_rows
    GROUP BY 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(n_rows - 9) AS n_rows_distance,
        distinct_adjuster_ids + n_rows AS distinct_adjuster_ids_plus_n_rows
    FROM grouped
)
SELECT
    first_claim_reported_at_year,
    n_rows,
    distinct_policy_ids,
    max_avg_claim_claimed_amount,
    distinct_adjuster_ids,
    n_rows_distance,
    distinct_adjuster_ids_plus_n_rows
FROM scored
ORDER BY n_rows, first_claim_reported_at_year DESC
