WITH cleaned_policys AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE end_date < DATE '2026-04-25'
),
enriched AS (
    -- Attach holder attributes
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM cleaned_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status
FROM enriched
WHERE start_date >= DATE '2024-12-03'
ORDER BY holder_id, id
LIMIT 10
