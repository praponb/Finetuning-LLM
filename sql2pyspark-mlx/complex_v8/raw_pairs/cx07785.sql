WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE premium >= 2033.8
),
unique_rows AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium, id) AS recency_rank
        FROM filtered_policys
    ) ranked
    WHERE recency_rank = 1
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.id) AS avg_claim_approved_amount,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at,
        (SELECT COUNT(CASE WHEN c.claim_type = 'flood' THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count
    FROM unique_rows s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        status,
        avg_claim_approved_amount,
        claim_claim_type_count,
        ROUND(AVG(premium) OVER (PARTITION BY claim_claim_type_count, DATE_TRUNC('MONTH', end_date) ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        ROUND(CUME_DIST() OVER w_claim_claim_type_count_by_cl, 3) AS claim_claim_type_count_cume_dist,
        LEAD(avg_claim_approved_amount, 2) OVER (PARTITION BY claim_claim_type_count, DATE_TRUNC('MONTH', end_date) ORDER BY premium DESC, id) AS next2_avg_claim_approved_amount,
        SUM(premium) OVER (PARTITION BY claim_claim_type_count, MONTH(end_date)) AS claim_claim_type_count_end_date_month_sum_premiu,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY claim_claim_type_count, MONTH(end_date))) AS distinct_holder_id_in_claim_claim_type_count_end
    FROM with_stats
    WINDOW w_claim_claim_type_count_by_cl AS (PARTITION BY claim_claim_type_count ORDER BY claim_claim_type_count, id)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.claim_claim_type_count,
        s.premium_avg_7,
        s.claim_claim_type_count_cume_dist,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM metrics s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score > 0.2
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claimed_amount > 5774.6
        )
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.claim_claim_type_count,
        s.premium_avg_7,
        s.claim_claim_type_count_cume_dist,
        s.holder_risk_score,
        p.joined_on AS holder_joined_on_2,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score_2
    FROM qualified s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.segment = 'RETAIL'
)
-- Final projection
SELECT
    id,
    holder_id,
    status,
    claim_claim_type_count,
    premium_avg_7,
    claim_claim_type_count_cume_dist,
    holder_risk_score,
    holder_joined_on_2,
    holder_segment,
    holder_risk_score_2,
    COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown
FROM with_parent_holder
ORDER BY claim_claim_type_count_cume_dist NULLS LAST, id
