WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE status = 'PAID'
      AND claim_type = 'medical'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(approved_amount) OVER w_reported_at_month_claim_type AS reported_at_month_claim_type_or_unknown_sum_appr,
        ROUND(AVG(approved_amount) OVER w_reported_at_month_claim_type, 2) AS reported_at_month_claim_type_or_unknown_avg_appr,
        ROUND(AVG(approved_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_3,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_all_by_approved_amount)) / 60, 1) AS minutes_since_prev_reported_at
    FROM recent_claims
    WINDOW w_reported_at_month_claim_type AS (PARTITION BY MONTH(reported_at), claim_type_or_unknown), w_all_by_approved_amount AS (ORDER BY approved_amount, id)
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE claim_type IN ('fire', 'theft')) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM with_trends s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
-- Subtotals with CUBE
SELECT
    policy_id,
    claim_claim_type_count,
    adjuster_id,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    MAX(reported_at) AS last_reported_at
FROM with_stats
GROUP BY CUBE(policy_id, claim_claim_type_count, adjuster_id)
ORDER BY policy_id NULLS LAST, claim_claim_type_count NULLS LAST, adjuster_id NULLS LAST
