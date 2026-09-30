WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 14:00:00'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        status,
        claim_type,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        SUM(CASE WHEN status <> 'APPROVED' THEN approved_amount ELSE 0 END) AS approved_amount_status_approved
    FROM cleaned
    WHERE status_claim_type_label IS NOT NULL
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(distinct_claim_types) OVER w_adjuster_id_by_distinct_clai AS next_distinct_claim_types,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY distinct_claim_types DESC, adjuster_id, status, claim_type), 3) AS distinct_claim_types_cume_dist,
        SUM(distinct_claim_types) OVER (PARTITION BY status ORDER BY distinct_claim_types DESC, adjuster_id, status, claim_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_claim_types
    FROM rollup_base
    WINDOW w_adjuster_id_by_distinct_clai AS (PARTITION BY adjuster_id ORDER BY distinct_claim_types DESC, adjuster_id, status, claim_type)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM ranked
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_type, status, next_distinct_claim_types
    FROM labeled
) src
PIVOT (
    COUNT(next_distinct_claim_types) AS count_v, SUM(next_distinct_claim_types) AS sum_v
    FOR status IN ('APPROVED', 'IN_REVIEW', 'REJECTED', 'OPEN')
)
