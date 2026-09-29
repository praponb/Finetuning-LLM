WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment IS NOT NULL
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.holder_joined_on,
        s.holder_segment,
        p.segment AS holder_segment_2,
        p.joined_on AS holder_joined_on_2
    FROM combined_details_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with ROLLUP
SELECT
    holder_id,
    COALESCE(holder_segment_2, 'ALL') AS holder_segment_2,
    COALESCE(holder_segment, 'ALL') AS holder_segment,
    ROUND(SUM(premium), 2) AS total_premium
FROM combined_details
GROUP BY ROLLUP(holder_id, holder_segment_2, holder_segment)
ORDER BY holder_id NULLS LAST, holder_segment_2 NULLS LAST, holder_segment NULLS LAST
