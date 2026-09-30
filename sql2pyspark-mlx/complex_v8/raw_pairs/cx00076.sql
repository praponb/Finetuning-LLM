WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE (end_date < DATE '2026-04-25' OR product IN ('TRAVEL', 'HEALTH', 'LIFE'))
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date < DATE '2025-05-28'
        )
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM without_matches s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT *
FROM (
    SELECT status, holder_segment, product, premium
    FROM combined_details_holder
) src
PIVOT (
    SUM(premium)
    FOR product IN ('HOME', 'LIFE', 'AUTO', 'TRAVEL')
)
ORDER BY status, holder_segment
