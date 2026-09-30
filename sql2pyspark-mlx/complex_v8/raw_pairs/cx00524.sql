WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        CASE WHEN product IN ('LIFE', 'HEALTH') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE status IN ('LAPSED', 'ACTIVE')
      AND end_date IS NOT NULL
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.status_clean,
        c.id AS claim_id,
        c.claim_date,
        c.reported_at AS claim_reported_at,
        c.claim_type,
        c.approved_amount AS claim_approved_amount
    FROM prepared_policys s
    JOIN claims c ON c.policy_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        premium,
        status,
        status_clean,
        claim_id,
        claim_date,
        claim_type,
        claim_approved_amount,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        COALESCE(premium, 0.0) AS premium_filled,
        UPPER(TRIM(status_clean)) AS status_clean_clean
    FROM with_children_claims
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_id,
        status,
        holder_id,
        ROUND(SUM(claim_approved_amount) / NULLIF(SUM(premium_filled), 0), 3) AS claim_approved_amount_to_premium_filled_ratio,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MIN(claim_approved_amount) AS min_claim_approved_amount,
        ROUND(SUM(premium_filled), 2) AS total_premium_filled
    FROM derived
    WHERE UPPER(TRIM(status_clean_clean)) LIKE 'L%'
    GROUP BY 1, 2, 3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(total_premium_filled - AVG(total_premium_filled) OVER w_holder_id_claim_id, 2) AS total_premium_filled_vs_holder_id_claim_id_avg,
        SIZE(COLLECT_SET(status) OVER w_holder_id_claim_id) AS distinct_status_in_holder_id_claim_id,
        ROW_NUMBER() OVER (ORDER BY distinct_ids DESC, claim_id, status, holder_id) AS rn
    FROM stats
    WINDOW w_holder_id_claim_id AS (PARTITION BY holder_id, claim_id)
),
with_children AS (
    -- One row per policy
    SELECT
        s.claim_id,
        s.status,
        s.holder_id,
        s.claim_approved_amount_to_premium_filled_ratio,
        s.min_claim_approved_amount,
        s.total_premium_filled,
        s.distinct_status_in_holder_id_claim_id,
        s.rn,
        p.id AS policy_id,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM with_trends s
    JOIN policies p ON p.holder_id = s.holder_id
)
SELECT
    policy_id,
    COALESCE(policy_status, 'ALL') AS policy_status,
    claim_id,
    GROUPING(policy_id) AS is_total_policy_id,
    COUNT(*) AS n_rows
FROM with_children
GROUP BY ROLLUP(policy_id, policy_status, claim_id)
ORDER BY policy_id NULLS LAST, policy_status NULLS LAST, claim_id NULLS LAST
