WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('HOME', 'HEALTH') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE holder_id IS NOT NULL
      AND start_date >= DATE '2024-08-27'
),
latest AS (
    -- Keep the latest row per product, product_group
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        product_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, product_group ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown_2
    FROM latest
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.status_or_unknown,
        s.status_or_unknown_2,
        p.segment AS holder_segment,
        p.holder_name,
        p.region AS holder_region
    FROM derived s
    JOIN policyholders p ON p.id = s.holder_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.status_or_unknown,
        s.holder_segment,
        s.holder_name,
        s.holder_region,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM enriched s
    JOIN claims c ON c.policy_id = s.id AND c.approved_amount >= 0.0
)
SELECT
    id,
    holder_id,
    product,
    holder_segment,
    holder_name,
    claim_id,
    claim_reported_at,
    claim_approved_amount
FROM line_level
WHERE status_or_unknown NOT IN ('LAPSED', 'EXPIRED')
ORDER BY product DESC, id DESC
