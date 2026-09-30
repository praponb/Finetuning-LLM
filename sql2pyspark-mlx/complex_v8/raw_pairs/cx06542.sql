WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE (approved_amount >= 0.0 OR adjuster_id <= 4)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id,
        DENSE_RANK() OVER w_policy_id_claim_date_month_b AS claimed_amount_dense_rank,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date) ORDER BY claimed_amount DESC, id) AS approved_amount_change,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id, MONTH(claim_date) ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date) ORDER BY claimed_amount DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4
    FROM filtered
    WINDOW w_policy_id_claim_date_month_b AS (PARTITION BY policy_id, MONTH(claim_date) ORDER BY claimed_amount DESC)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(status)) AS status_len,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM metrics
    WHERE policy_id IS NOT NULL
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) FILTER (WHERE status <> 'REJECTED') AS n_status_rejected,
        ROUND(AVG(rows_in_adjuster_id_policy_id), 2) AS avg_rows_in_adjuster_id_policy_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT status) AS distinct_status
    FROM screened
    GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM')
),
flattened AS (
    -- One row per status_list_item
    SELECT
        s.claim_date_ym,
        s.n_status_rejected,
        s.avg_rows_in_adjuster_id_policy_id,
        s.n_rows,
        s.distinct_status,
        t.status_list_item
    FROM stats s
    LATERAL VIEW OUTER EXPLODE(s.status_list) t AS status_list_item
)
-- Final projection
SELECT
    claim_date_ym,
    n_status_rejected,
    avg_rows_in_adjuster_id_policy_id,
    n_rows,
    distinct_status,
    status_list_item
FROM flattened
ORDER BY n_status_rejected NULLS LAST
