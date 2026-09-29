WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CASE WHEN product IN ('TRAVEL', 'HOME') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM cleaned_policys
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.days_since_start_date,
        s.product_or_unknown,
        s.status_slug,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount
    FROM derived s
    LEFT JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    product_or_unknown,
    status_slug,
    claim_id,
    claim_status,
    claim_reported_at,
    claim_approved_amount,
    claim_claimed_amount
FROM line_level
WHERE status <> 'EXPIRED'
