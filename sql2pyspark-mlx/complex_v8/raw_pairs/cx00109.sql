WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        ROUND(premium * 0.25, 2) AS premium_adj,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE premium >= 2096.6
),
with_details_holder AS (
    -- Join policyholders
    SELECT s.*, p.region AS holder_region, p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, premium, premium_adj
    FROM with_details_holder
) src
UNPIVOT (
    reading FOR metric IN (premium, premium_adj)
) unp
ORDER BY id, metric
