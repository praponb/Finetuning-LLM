WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM policies
    WHERE start_date >= DATE '2024-10-22'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        MAX(approved_amount) AS max_claim_approved_amount,
        SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.max_claim_approved_amount,
        c.total_claim_claimed_amount
    FROM cleaned_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM profiled s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.product_prefix,
        s.claim_count,
        s.total_claim_claimed_amount,
        s.policy_count,
        p.segment AS holder_segment
    FROM summarized_rows s
    JOIN policyholders p ON p.id = s.holder_id AND p.holder_name IS NOT NULL
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status,
    product_prefix,
    total_claim_claimed_amount,
    policy_count,
    holder_segment,
    SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
    CONCAT_WS('-', product_prefix, holder_segment) AS product_prefix_holder_segment_label
FROM joined
ORDER BY start_date DESC
