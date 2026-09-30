WITH filtered_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        region,
        joined_on,
        segment,
        CONCAT_WS('-', segment, holder_name) AS segment_holder_name_label,
        LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
    FROM policyholders
    WHERE (region <> 'South' OR UPPER(TRIM(holder_name)) LIKE 'L%')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(joined_on) OVER w_segment_region_by_joined_on AS prev_joined_on,
        RANK() OVER w_segment_region_by_joined_on AS joined_on_rank,
        FIRST_VALUE(joined_on) OVER (PARTITION BY segment, region ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on,
        DATEDIFF(joined_on, LAG(joined_on) OVER (PARTITION BY segment, region ORDER BY joined_on DESC, id)) AS days_since_prev_joined_on
    FROM filtered_holders
    WINDOW w_segment_region_by_joined_on AS (PARTITION BY segment, region ORDER BY joined_on DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(holder_name_slug)) AS holder_name_slug_clean,
        DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on,
        CONCAT(segment, '#', CAST(joined_on_rank AS STRING)) AS segment_ref
    FROM ranked
    WHERE joined_on_rank BETWEEN 2 AND 7
),
exploded_detail_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name_slug,
        s.days_since_joined_on,
        s.segment_ref,
        p.id AS policy_id,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM labeled s
    JOIN policies p ON p.holder_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        policy_premium + days_since_joined_on AS policy_premium_plus_days_since_joined_on
    FROM exploded_detail_policys
),
joined AS (
    -- Join policies
    SELECT s.*, p.holder_id AS policy_holder_id
    FROM tagged s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.holder_id IS NOT NULL
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_name_slug,
        s.days_since_joined_on,
        s.segment_ref,
        s.policy_id,
        s.policy_product,
        s.policy_start_date,
        s.policy_premium,
        s.policy_status,
        s.policy_holder_id,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        po.status AS policy_status_2
    FROM joined s
    JOIN policyholders p ON p.id = s.policy_holder_id
    LEFT JOIN policies po ON po.id = s.policy_id
)
-- Final projection
SELECT
    id,
    holder_name_slug,
    days_since_joined_on,
    segment_ref,
    policy_id,
    policy_product,
    policy_start_date,
    policy_status,
    policy_holder_id,
    holder_risk_score,
    holder_joined_on,
    policy_status_2,
    LENGTH(TRIM(holder_name_slug)) AS holder_name_slug_len,
    SUBSTRING(TRIM(policy_status), 1, 3) AS policy_status_prefix
FROM enriched_holder
ORDER BY holder_name_slug_len, id, policy_id
LIMIT 5
