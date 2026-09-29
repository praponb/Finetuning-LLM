WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE approved_amount >= 0.0
      AND status IN ('APPROVED', 'REJECTED')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status), 2) AS claimed_amount_vs_status_avg,
        approved_amount_filled - LAG(approved_amount_filled) OVER (PARTITION BY claim_type, adjuster_id ORDER BY reported_at, id) AS approved_amount_filled_change,
        LEAD(reported_at, 2) OVER w_claim_type_adjuster_id_by_re AS next2_reported_at
    FROM scoped_claims
    WINDOW w_claim_type_adjuster_id_by_re AS (PARTITION BY claim_type, adjuster_id ORDER BY reported_at, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount_filled_change,
        next2_reported_at,
        RANK() OVER (ORDER BY approved_amount_filled_change DESC, id) AS approved_amount_filled_change_rank,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        LAST_VALUE(approved_amount_filled) OVER (ORDER BY approved_amount_filled_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount_filled,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status)) AS distinct_adjuster_id_in_status
    FROM ranked
)
SELECT
    distinct_adjuster_id_in_status,
    approved_amount_filled_change_rank,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT claim_type) AS distinct_claim_types
FROM windowed
GROUP BY GROUPING SETS ((distinct_adjuster_id_in_status, approved_amount_filled_change_rank, claim_type), (distinct_adjuster_id_in_status), (approved_amount_filled_change_rank), ())
ORDER BY distinct_adjuster_id_in_status NULLS LAST, approved_amount_filled_change_rank NULLS LAST, claim_type NULLS LAST
