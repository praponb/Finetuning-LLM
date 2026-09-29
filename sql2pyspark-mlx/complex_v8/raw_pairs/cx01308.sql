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
        ABS(claimed_amount - 20505.7) AS claimed_amount_distance,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE YEAR(claim_date) = 2024
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(CASE WHEN status <> 'REJECTED' THEN 1 END) AS claim_status_count,
        MIN(reported_at) AS first_claim_reported_at,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.first_claim_reported_at,
        c.max_claim_approved_amount
    FROM valid_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
grouped AS (
    -- Group-level totals
    SELECT
        claim_type,
        COUNT(*) FILTER (WHERE status NOT IN ('REJECTED', 'PAID')) AS n_status_not_in_rejected_paid,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(claimed_amount_distance), 2) AS stddev_claimed_amount_distance
    FROM summarized_rows
    WHERE claim_type <> 'fire'
    GROUP BY 1
    HAVING MAX(claimed_amount) > 10673.3
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_ids,
    n_status_not_in_rejected_paid,
    GROUPING(distinct_ids) AS is_total_distinct_ids,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    COUNT(*) AS cnt
FROM grouped
GROUP BY GROUPING SETS ((distinct_ids, n_status_not_in_rejected_paid), (distinct_ids), ())
