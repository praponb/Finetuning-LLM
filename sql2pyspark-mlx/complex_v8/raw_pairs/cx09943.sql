WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
),
with_details_holder AS (
    -- Join policyholders
    SELECT s.*, p.segment AS holder_segment
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on < DATE '2024-12-21'
),
status_stats AS (
    -- Median premium per status
    SELECT status, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM with_details_holder
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM with_details_holder s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium >= g.median_premium
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(median_premium / NULLIF(premium_minus_median, 0), 3) AS median_premium_per_premium_minus_median,
        CASE WHEN holder_segment IN ('RETAIL', 'VIP') THEN 'group_a' WHEN holder_segment = 'SME' THEN 'group_b' ELSE 'other' END AS holder_segment_group
    FROM above_average
    WHERE status IN ('ACTIVE', 'LAPSED')
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        s.premium_filled,
        s.holder_segment,
        s.median_premium,
        s.premium_minus_median,
        s.median_premium_per_premium_minus_median,
        s.holder_segment_group,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM enriched s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.holder_name IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per holder_segment, holder_segment_group
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_filled,
        holder_segment,
        median_premium,
        premium_minus_median,
        median_premium_per_premium_minus_median,
        holder_segment_group,
        holder_joined_on,
        holder_risk_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_segment, holder_segment_group ORDER BY premium DESC, id DESC) AS rn
        FROM joined_holder
    ) ranked
    WHERE rn = 1
),
deduped AS (
    -- Keep the latest row per holder_segment_group
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_filled,
        holder_segment,
        median_premium,
        premium_minus_median,
        median_premium_per_premium_minus_median,
        holder_segment_group,
        holder_joined_on,
        holder_risk_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_segment_group ORDER BY holder_joined_on DESC, id DESC) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    premium_filled,
    holder_segment,
    median_premium,
    premium_minus_median,
    median_premium_per_premium_minus_median,
    holder_segment_group,
    holder_joined_on,
    holder_risk_score,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
    CONCAT_WS(': ', product, status) AS product_status_label
FROM deduped
ORDER BY id, holder_segment_group DESC
