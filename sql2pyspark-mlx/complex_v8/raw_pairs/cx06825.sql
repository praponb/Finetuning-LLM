WITH prepared AS (
    -- Scope the claims rows
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
        CASE WHEN status IN ('APPROVED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group,
        TO_DATE(reported_at) AS reported_at_date,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE claim_type NOT IN ('flood', 'medical')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_id,
        status_group,
        ROUND(100 * COUNT(CASE WHEN status = 'IN_REVIEW' THEN 1 END) / COUNT(*), 1) AS pct_status_in_review,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS approved_amount_to_claimed_amount_ratio,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        MIN(approved_amount) AS min_approved_amount,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM prepared
    WHERE policy_id IS NOT NULL
    GROUP BY 1, 2
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.adjuster_id,
        s.status_group,
        s.pct_status_in_review,
        s.distinct_claim_types,
        s.min_approved_amount,
        s.distinct_adjuster_ids,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM per_group s
    JOIN adjusters a ON a.id = s.adjuster_id AND LOWER(a.adjuster_name) LIKE '% s%'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_level, status_group, min_approved_amount
    FROM joined
) src
PIVOT (
    SUM(min_approved_amount)
    FOR status_group IN ('group_a', 'group_b', 'other')
)
