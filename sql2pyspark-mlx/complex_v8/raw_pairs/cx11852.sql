WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LENGTH(TRIM(status)) AS status_len,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE premium < 3544.1
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.region AS holder_region,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on < DATE '2025-07-03'
    WHERE p.holder_name IS NOT NULL
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM combined_details_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date < DATE '2025-01-10'
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        MIN(reported_at) AS first_claim_reported_at,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount,
        c.first_claim_reported_at,
        c.first_claim_date
    FROM filtered s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(product_slug), 1, 4) AS product_slug_prefix,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM with_counts
    WHERE min_claim_claimed_amount <= 45010.0
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown
    FROM scored
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status_len,
    days_start_date_to_end_date,
    product_slug,
    holder_name,
    holder_risk_score,
    min_claim_claimed_amount,
    first_claim_reported_at,
    end_date_week,
    holder_region_or_unknown
FROM labeled
WHERE first_claim_date < DATE '2025-02-01'
ORDER BY holder_region_or_unknown, start_date DESC NULLS FIRST
