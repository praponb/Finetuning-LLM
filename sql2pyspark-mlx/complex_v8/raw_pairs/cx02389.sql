WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date
    FROM policies
    WHERE end_date IS NOT NULL
      AND product = 'TRAVEL'
)
SELECT holder_id AS entity_id, end_date AS day, status AS label
FROM prepared_policys
WHERE end_date < DATE '2026-09-19'
  AND status <> 'ACTIVE'
UNION
SELECT policy_id AS entity_id, claim_date AS day, claim_type AS label
FROM claims
ORDER BY entity_id DESC
