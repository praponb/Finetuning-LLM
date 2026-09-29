WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status,
        c.claim_type
    FROM prepared s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_type IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per status, claim_status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        claim_id,
        claim_approved_amount,
        claim_status,
        claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, claim_status ORDER BY claimed_amount DESC, id DESC, claim_id DESC) AS rn
        FROM exploded_detail
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claimed_amount,
        status,
        claim_id,
        claim_approved_amount,
        claim_type,
        NTH_VALUE(claim_approved_amount, 2) OVER (PARTITION BY policy_id, MONTH(reported_at) ORDER BY claim_approved_amount ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_approved_amount,
        SIZE(COLLECT_SET(claim_id) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at))) AS distinct_claim_id_in_reported_at_month,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at)), 2) AS claimed_amount_vs_reported_at_month_avg,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at)) AS rows_in_reported_at_month,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at))) AS distinct_adjuster_id_in_reported_at_month
    FROM unique_rows
),
rollup_base AS (
    -- Summarise per group
    SELECT
        distinct_claim_id_in_reported_at_month,
        claim_type,
        COUNT(*) AS cnt,
        ROUND(STDDEV(claimed_amount_vs_reported_at_month_avg), 2) AS stddev_claimed_amount_vs_reported_at_month_avg,
        SUM(claimed_amount) FILTER (WHERE claim_type IN ('medical', 'flood', 'delay')) AS claimed_amount_claim_type_in_medical_flood,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        PERCENTILE_APPROX(rows_in_reported_at_month, 0.5) AS median_rows_in_reported_at_month
    FROM windowed
    GROUP BY distinct_claim_id_in_reported_at_month, claim_type
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE claimed_amount_claim_type_in_medical_flood > (SELECT MIN(claimed_amount_claim_type_in_medical_flood) FROM rollup_base)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN cnt >= 9 THEN 'HIGH' WHEN cnt >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier,
        FLOOR(cnt / 2) * 2 AS cnt_bucket,
        CASE WHEN cnt >= 10 THEN 'large' WHEN cnt >= 3 THEN 'medium' ELSE 'small' END AS cnt_tier_2
    FROM with_activity
    WHERE cnt < 10
)
-- Final projection
SELECT
    distinct_claim_id_in_reported_at_month,
    claim_type,
    cnt,
    stddev_claimed_amount_vs_reported_at_month_avg,
    claimed_amount_claim_type_in_medical_flood,
    median_rows_in_reported_at_month,
    cnt_tier,
    cnt_bucket,
    cnt_tier_2,
    COALESCE(cnt_tier_2, 'UNKNOWN') AS cnt_tier_2_or_unknown
FROM scored
WHERE cnt_tier = 'MEDIUM'
ORDER BY stddev_claimed_amount_vs_reported_at_month_avg DESC, cnt_tier DESC
