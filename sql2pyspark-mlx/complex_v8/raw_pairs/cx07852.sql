WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        CASE WHEN status IN ('OPEN', 'REJECTED') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE (claim_date >= DATE '2024-07-18' OR policy_id >= 16)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(reported_at) AS reported_at_day,
        policy_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(DISTINCT claim_type_or_unknown) AS distinct_claim_type_or_unknowns,
        ROUND(STDDEV(approved_amount_per_claimed_amount), 2) AS stddev_approved_amount_per_claimed_amount
    FROM base
    WHERE approved_amount_per_claimed_amount > 2.0
    GROUP BY TO_DATE(reported_at), policy_id
)
SELECT
    reported_at_day,
    policy_id,
    n_rows,
    distinct_policy_ids,
    stddev_approved_amount_per_claimed_amount,
    COALESCE(n_rows, 0) AS n_rows_filled
FROM rollup_base
WHERE distinct_claim_type_or_unknowns < 6
ORDER BY stddev_approved_amount_per_claimed_amount DESC, n_rows
