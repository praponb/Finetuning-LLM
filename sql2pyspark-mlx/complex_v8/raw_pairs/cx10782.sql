WITH prepared_claims AS (
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
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE policy_id IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        SUM(approved_amount) AS total_claim_approved_amount,
        COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.total_claim_approved_amount,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM prepared_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
policy_id_stats AS (
    -- Avg approved_amount per policy_id
    SELECT policy_id, AVG(approved_amount) AS avg_approved_amount
    FROM with_stats
    GROUP BY policy_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM with_stats s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount > g.avg_approved_amount
)
SELECT
    claim_count_2,
    policy_id,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT status) AS distinct_status
FROM compared
GROUP BY CUBE(claim_count_2, policy_id)
ORDER BY claim_count_2 NULLS LAST, policy_id NULLS LAST
