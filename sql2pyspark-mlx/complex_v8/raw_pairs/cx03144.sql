WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
    WHERE product <> 'HOME'
),
latest AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM cleaned_policys
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM latest s
    JOIN policyholders p ON p.id = s.holder_id AND p.region <> 'West'
),
labeled AS (
    -- Derive helper columns
    SELECT *, COALESCE(holder_risk_score, 0.0) AS holder_risk_score_filled
    FROM joined
    WHERE product = 'AUTO'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.holder_name AS holder_holder_name,
        p.region AS holder_region
    FROM labeled s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment = 'VIP'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count, COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM with_parent s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.*,
        p.segment AS holder_segment,
        p.region AS holder_region_2,
        p.holder_name AS holder_holder_name_2
    FROM summarized_rows s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    end_date,
    holder_name,
    holder_risk_score,
    holder_risk_score_filled,
    holder_joined_on,
    holder_region,
    claim_count,
    claim_count_2,
    holder_segment,
    holder_region_2,
    holder_holder_name_2
FROM combined_details
WHERE holder_region_2 = 'Central'
ORDER BY holder_segment
