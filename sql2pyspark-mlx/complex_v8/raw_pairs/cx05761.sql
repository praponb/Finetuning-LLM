WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        CASE WHEN premium >= 4417.0 THEN 'A' WHEN premium >= 2013.8 THEN 'B' ELSE 'C' END AS premium_tier,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM policies
    WHERE start_date < DATE '2025-02-14'
      AND product = 'TRAVEL'
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        premium_tier,
        start_date_quarter,
        start_date_quarter + INTERVAL 7 DAYS AS start_date_quarter_plus_7d,
        COALESCE(premium_tier, 'UNKNOWN') AS premium_tier_or_unknown
    FROM prepared
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium_tier,
        s.start_date_quarter,
        s.premium_tier_or_unknown,
        p.segment AS holder_segment
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    premium_tier,
    premium_tier_or_unknown,
    holder_segment,
    CASE WHEN premium_tier IN ('B', 'A') THEN 'group_a' WHEN premium_tier = 'C' THEN 'group_b' ELSE 'other' END AS premium_tier_group
FROM enriched_holder
ORDER BY holder_segment NULLS LAST
