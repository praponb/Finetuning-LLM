WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ABS(premium - 2721.8) AS premium_distance
    FROM policies
    WHERE holder_id <= 2
      AND status NOT IN ('LAPSED', 'ACTIVE')
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(premium_distance - 2750.0) AS premium_distance_distance,
        COALESCE(premium_distance, 0.0) AS premium_distance_filled,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM prepared_policys
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.premium_distance,
        s.premium_distance_distance,
        s.product_ref,
        p.holder_name,
        p.segment AS holder_segment
    FROM derived s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT DISTINCT holder_id
FROM combined_details
ORDER BY holder_id
