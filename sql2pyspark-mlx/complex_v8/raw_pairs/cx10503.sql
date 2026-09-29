WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM policies
    WHERE end_date BETWEEN DATE '2025-09-27' AND DATE '2026-10-23'
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.end_date_dayofweek,
        p.holder_name,
        p.segment AS holder_segment
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.end_date_dayofweek,
        p.holder_name AS holder_holder_name,
        p.segment AS holder_segment_2
    FROM with_details_holder s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    end_date_dayofweek,
    holder_holder_name
FROM with_parent
WHERE product IN ('AUTO', 'HOME', 'TRAVEL')
ORDER BY end_date_dayofweek, id
LIMIT 10
