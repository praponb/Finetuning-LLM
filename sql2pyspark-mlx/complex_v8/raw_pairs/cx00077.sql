WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE policy_id IS NOT NULL
),
unique_rows AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        status_or_unknown,
        status_claim_type_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY status_or_unknown ORDER BY approved_amount ASC, id)) AS days_since_prev_claim_date,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status)) AS distinct_adjuster_id_in_status,
        MIN(claimed_amount) OVER (PARTITION BY status) AS status_min_claimed_amount
    FROM unique_rows
)
SELECT
    COALESCE(status_or_unknown, 'ALL') AS status_or_unknown,
    COALESCE(status, 'ALL') AS status,
    ROUND(100 * COUNT(CASE WHEN status = 'PAID' THEN 1 END) / COUNT(*), 1) AS pct_status_paid
FROM ranked
GROUP BY GROUPING SETS ((status_or_unknown, status), (status_or_unknown), (status))
ORDER BY status_or_unknown NULLS LAST, status NULLS LAST
