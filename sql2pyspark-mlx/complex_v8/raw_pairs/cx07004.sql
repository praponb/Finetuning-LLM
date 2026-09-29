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
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM policies
    WHERE end_date IS NOT NULL
      AND status IN ('EXPIRED', 'ACTIVE')
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.end_date_label,
        p.joined_on AS holder_joined_on
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, product, premium
    FROM enriched_holder
) src
PIVOT (
    MIN(premium)
    FOR product IN ('TRAVEL', 'HOME', 'HEALTH')
)
