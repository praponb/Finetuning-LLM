WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        LOWER(TRIM(product)) AS product_clean,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
    WHERE holder_id IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        product_clean,
        start_date_month_start,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label,
        CASE WHEN premium >= 3544.1 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM cleaned
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.product_clean,
        s.start_date_month_start,
        s.premium_bucket,
        s.premium_tier,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM derived s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM with_parent_holder
    WHERE premium_bucket > (SELECT MIN(premium_bucket) FROM with_parent_holder)
)
-- Final projection
SELECT
    id,
    premium,
    product_clean,
    start_date_month_start,
    premium_bucket,
    premium_tier,
    holder_region,
    ROUND(premium * 0.9, 2) AS premium_adj
FROM screened
ORDER BY premium DESC, id DESC NULLS FIRST
LIMIT 10
