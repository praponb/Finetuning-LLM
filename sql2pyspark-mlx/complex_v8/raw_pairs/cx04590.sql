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
        reported_at,
        INITCAP(TRIM(status)) AS status_clean,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
    WHERE claimed_amount < 44921.3
      AND adjuster_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        status_clean,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        CASE WHEN claimed_amount >= 40638.4 THEN 'A' WHEN claimed_amount >= 13635.1 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM prepared
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY MONTH(claim_date))) AS distinct_policy_id_in_claim_date_month,
        MAX(approved_amount) OVER w_claim_date_month AS claim_date_month_max_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY MONTH(claim_date)), 2) AS approved_amount_vs_claim_date_month_avg,
        FIRST_VALUE(claim_date) OVER (ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY MONTH(claim_date))) AS distinct_adjuster_id_in_claim_date_month,
        ROW_NUMBER() OVER w_all_by_approved_amount AS seq_no
    FROM labeled
    WINDOW w_claim_date_month AS (PARTITION BY MONTH(claim_date)), w_all_by_approved_amount AS (ORDER BY approved_amount DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type_prefix,
    claim_type_or_unknown,
    approved_amount_vs_claim_date_month_avg,
    first_claim_date,
    distinct_adjuster_id_in_claim_date_month,
    DATEDIFF(claim_date, first_claim_date) AS days_first_claim_date_to_claim_date
FROM metrics
WHERE distinct_policy_id_in_claim_date_month >= 4
ORDER BY adjuster_id NULLS LAST
