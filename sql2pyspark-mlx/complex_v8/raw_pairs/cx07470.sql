WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 20880.4 THEN 'HIGH' WHEN approved_amount >= 2272.7 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE claim_type = 'fire'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        CASE WHEN approved_amount >= 19337.6 THEN 'A' WHEN approved_amount >= 0.0 THEN 'B' ELSE 'C' END AS approved_amount_tier_2,
        policy_id,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MAX(reported_at) AS last_reported_at,
        COUNT(*) AS cnt,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
    FROM base
    GROUP BY CASE WHEN approved_amount >= 19337.6 THEN 'A' WHEN approved_amount >= 0.0 THEN 'B' ELSE 'C' END, policy_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        approved_amount_tier_2,
        policy_id,
        pct_status_is_not_null,
        claim_type_list,
        last_reported_at,
        claim_date_span_days,
        ROUND((UNIX_TIMESTAMP(last_reported_at) - UNIX_TIMESTAMP(LAG(last_reported_at) OVER (ORDER BY pct_status_is_not_null NULLS LAST, approved_amount_tier_2, policy_id))) / 60, 1) AS minutes_since_prev_last_reported_at,
        ROW_NUMBER() OVER (ORDER BY pct_status_is_not_null NULLS LAST, approved_amount_tier_2, policy_id) AS seq_no
    FROM rollup_base
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type = 'delay') FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count
    FROM ranked s
)
SELECT *
FROM (
    SELECT claim_claim_type_count, last_reported_at, approved_amount_tier_2, claim_date_span_days
    FROM with_counts
) src
PIVOT (
    SUM(claim_date_span_days) AS total, COUNT(claim_date_span_days) AS n
    FOR approved_amount_tier_2 IN ('C', 'B', 'A')
)
ORDER BY claim_claim_type_count, last_reported_at
