WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE start_date BETWEEN DATE '2024-05-07' AND DATE '2025-03-23'
)
SELECT id
FROM base
EXCEPT
SELECT policy_id
FROM claims
